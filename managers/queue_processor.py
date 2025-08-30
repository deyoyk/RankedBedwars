import asyncio
from typing import Dict, List, Set, Optional, Tuple
from datetime import datetime
import discord
from managers.database_manager import DatabaseManager
from managers.party_manager import PartyManager
from utils.embed_builder import EmbedBuilder
import random
import string
from bson import Timestamp  
from managers.mute_manager import MuteManager
import os
import time
import logging

from collections import defaultdict
from discord import app_commands
from managers.workermanager import WorkerManager


try:
    from managers.websocket_manager import WebSocketManager
except ImportError:
    WebSocketManager = None

class QueueProcessor:
    def __init__(self, bot):
        self.bot = bot
        self.db_manager = DatabaseManager()
        self.party_manager = PartyManager(config_file='configs/config.yml', db_manager=self.db_manager)
        self.embed_builder = EmbedBuilder()
        self.mute_manager = MuteManager(self.bot)
        self.guild_id = int(self.bot.config['bot']['guildid'])
        
        
        self.queues = {}  
        self.player_queue_map = {}  
        self.queue_locks = {}  
        self.processing_flags = {}  
        self.player_locks = defaultdict(asyncio.Lock)  
        self.queue_tasks = {}  
        self.continuous_queue_tasks = {}  
        
        
        self._load_queue_processor_config()

        
        self.worker_manager = getattr(self.bot, 'worker_manager', None)
        
        
        self.websocket_enabled = self.bot.config.get('websocket', {}).get('enabled', False)
        self.ws_manager = getattr(self.bot, 'websocket_manager', None) if self.websocket_enabled else None
        
        
        self._init_continuous_processing()

        logging.info(f"QueueProcessor initialized with high-capacity configuration and continuous processing. WebSocket enabled: {self.websocket_enabled}")
    
    def _load_queue_processor_config(self):
        try:
            queue_config = self.bot.config.get('queue_processor', {})
            
            
            self.batch_size = queue_config.get('batch_size', 100)
            self.processing_cooldown = queue_config.get('processing_cooldown', 2.0)
            self.queue_last_processed = {}  
            self.continuous_check_interval = queue_config.get('continuous_check_interval', 5.0)
            self.partial_batch_wait_time = queue_config.get('partial_batch_wait_time', 60.0)
            self.min_players_for_partial_game = queue_config.get('min_players_for_partial_game', 4)
            
            
            self.players_in_game_creation = set()
            
            logging.info(f"Loaded queue processor config: check interval={self.continuous_check_interval}s, "
                        f"partial batch wait={self.partial_batch_wait_time}s, "
                        f"min players={self.min_players_for_partial_game}")
                        
        except Exception as e:
            logging.error(f"Error loading queue processor config, using defaults: {e}")
            
            self.batch_size = 100
            self.processing_cooldown = 2.0
            self.queue_last_processed = {}
            self.continuous_check_interval = 5.0
            self.partial_batch_wait_time = 60.0
            self.min_players_for_partial_game = 4
            self.players_in_game_creation = set()
    
    async def acquire_player_locks(self, player_ids: List[int]) -> bool:
        
        sorted_ids = sorted(player_ids)
        locks_acquired = []
        
        try:
            for player_id in sorted_ids:
                
                if player_id in self.players_in_game_creation:
                    
                    for acquired_id in locks_acquired:
                        self.player_locks[acquired_id].release()
                    return False
                
                
                await asyncio.wait_for(self.player_locks[player_id].acquire(), timeout=0.5)
                locks_acquired.append(player_id)
            
            return True
        except asyncio.TimeoutError:
            
            for player_id in locks_acquired:
                self.player_locks[player_id].release()
            return False
    
    def release_player_locks(self, player_ids: List[int]):
        for player_id in player_ids:
            if self.player_locks[player_id].locked():
                self.player_locks[player_id].release()
    async def process_queue_join(self, user_id: int, channel_id: str) -> None:
        try:
            
            if channel_id not in self.queue_locks:
                self.queue_locks[channel_id] = asyncio.Lock()
            
            
            async with self.player_locks[user_id]:
                
                if user_id in self.player_queue_map:
                    current_queue = self.player_queue_map[user_id]
                    if current_queue == channel_id:
                        logging.debug(f"User {user_id} is already in queue {channel_id}")
                        return
                    else:
                        
                        await self.process_queue_leave(user_id, current_queue)
                
                
                async with self.queue_locks[channel_id]:
                    
                    queue_settings = self.db_manager.find_one('queues', {'channelid': str(channel_id)})
                    if not queue_settings:
                        logging.warning(f"No queue settings found for channel {channel_id} in database")
                        return
                    
                    
                    channel = self.bot.get_channel(int(channel_id))
                    if not channel:
                        logging.warning(f"Could not find channel with ID {channel_id}")
                        return
                    
                    
                    if channel_id not in self.queues:
                        self.queues[channel_id] = {
                            'players': set(),
                            'max_players': queue_settings['maxplayers'],
                            'parties': [],
                            'was_full': False,
                            'last_processed': 0,
                            'last_partial_check': 0
                        }
                        
                        await self.start_continuous_processing(channel_id)
                    
                    
                    party = self.party_manager.get_party_by_member(str(user_id))
                    voice_members = set(member.id for member in channel.members)
                    
                    if party:
                        
                        party_members = set(int(member_id) for member_id in party['members'])
                        
                        
                        if len(party_members) > queue_settings['maxplayers'] // 2:
                            logging.debug(f"Party too large for queue {channel_id}")
                            return
                        
                        
                        present_members = party_members & voice_members
                        if not present_members:
                            logging.debug(f"No party members present in voice channel {channel_id}")
                            return
                        
                        
                        for member_id in present_members:
                            if member_id not in self.player_queue_map:
                                self.queues[channel_id]['players'].add(member_id)
                                self.player_queue_map[member_id] = channel_id
                        
                        
                        self.queues[channel_id]['parties'].append({
                            'members': list(present_members),
                            'size': len(present_members),
                            'join_time': time.time()
                        })
                        logging.debug(f"Added party with {len(present_members)} members to queue {channel_id}")
                    else:
                        
                        self.queues[channel_id]['players'].add(user_id)
                        self.player_queue_map[user_id] = channel_id
                        logging.debug(f"Added solo player {user_id} to queue {channel_id}")
                    
                    
                    queue = self.queues[channel_id]
                    if len(queue['players']) >= queue['max_players']:
                        
                        current_time = time.time()
                        if current_time - queue.get('last_processed', 0) >= self.processing_cooldown:
                            queue['last_processed'] = current_time
                            
                            asyncio.create_task(self.process_queue(channel_id, allow_partial=False))
                            logging.debug(f"Immediately processing full queue {channel_id}")
        
        except Exception as e:
            logging.error(f"Error processing queue join: {e}", exc_info=True)
    
    async def process_queue_leave(self, user_id: int, channel_id: str, new_channel_id: str = None) -> None:
        try:
            
            if channel_id not in self.queues:
                return
            
            
            async with self.player_locks[user_id]:
                
                if user_id not in self.queues[channel_id]['players']:
                    return
                
                
                async with self.queue_locks[channel_id]:
                    
                    party = self.party_manager.get_party_by_member(str(user_id))
                    if party:
                        
                        for member_id in party['members']:
                            member_id_int = int(member_id)
                            if member_id_int in self.queues[channel_id]['players']:
                                self.queues[channel_id]['players'].discard(member_id_int)
                                self.player_queue_map.pop(member_id_int, None)
                        
                        
                        self.queues[channel_id]['parties'] = [
                            p for p in self.queues[channel_id]['parties']
                            if str(user_id) not in [str(m) for m in p['members']]
                        ]
                    else:
                        
                        self.queues[channel_id]['players'].discard(user_id)
                        self.player_queue_map.pop(user_id, None)
            
            
            if new_channel_id:
                await self.process_queue_join(user_id, new_channel_id)
        
        except Exception as e:
            logging.error(f"Error processing queue leave: {e}", exc_info=True)
    
    def get_queue_wait_time(self, channel_id: str) -> float:
        if channel_id not in self.queues or not self.queues[channel_id]['parties']:
            return 0
        
        current_time = time.time()
        
        earliest_join = min([p.get('join_time', current_time) for p in self.queues[channel_id]['parties']])
        return current_time - earliest_join
    
    def get_queue_status(self, channel_id: str) -> Dict:
        if channel_id not in self.queues:
            return {'exists': False}
        
        queue = self.queues[channel_id]
        player_count = len(queue['players'])
        wait_time = self.get_queue_wait_time(channel_id)
        
        return {
            'exists': True,
            'player_count': player_count,
            'max_players': queue['max_players'],
            'wait_time': wait_time,
            'parties': len(queue['parties']),
            'is_full': player_count >= queue['max_players'],
            'can_start_partial': (player_count >= self.min_players_for_partial_game and 
                                 wait_time >= self.partial_batch_wait_time)
        }
    
    def should_process_partial_batch(self, channel_id: str) -> bool:
        if channel_id not in self.queues:
            return False
        
        queue = self.queues[channel_id]
        player_count = len(queue['players'])
        
        
        queue_settings = self.db_manager.find_one('queues', {'channelid': str(channel_id)})
        
    async def check_player_online(self, ign: str) -> bool:
        if not self.websocket_enabled or not self.ws_manager:
            
            logging.debug(f"WebSocket not enabled, assuming player {ign} is online")
            return True
            
        try:
            
            player_handler = getattr(self.ws_manager, 'player_handler', None)
            if player_handler:
                
                is_online = await asyncio.wait_for(
                    player_handler.check_player_online(ign), 
                    timeout=5.0  
                )
                logging.info(f"WebSocket player check for {ign}: {'online' if is_online else 'offline'}")
                return is_online
            else:
                logging.warning("WebSocket player_handler not available, assuming player is online")
                return True
        except asyncio.TimeoutError:
            logging.warning(f"WebSocket player check timed out for {ign}, assuming player is online")
            return True
        except Exception as e:
            logging.error(f"Error checking player online status: {e}", exc_info=True)
            return True  
        if not queue_settings:
            return False
            
        
        return player_count >= queue_settings['maxplayers']
    
    def _create_batch(self, players: Set[int], parties: List[Set[int]], max_players: int) -> Tuple[Set[int], List[Set[int]]]:
        batch = set()
        used_parties = []
        
        
        sorted_parties = sorted(parties, key=len, reverse=True)
        for party in sorted_parties:
            if len(batch) + len(party) <= max_players:
                batch.update(party)
                used_parties.append(party)
        
        
        solo_players = players - set().union(*parties) if parties else players
        solo_list = list(solo_players)
        random.shuffle(solo_list)
        
        for player_id in solo_list:
            if len(batch) < max_players:
                batch.add(player_id)
            else:
                break
        
        return batch, used_parties
    
    async def _send_start_game_websocket_event(self, game_id: str, queue_settings: dict, team1: List[int], team2: List[int]):
        if not self.websocket_enabled or not self.ws_manager:
            return

        try:
            team1_data = []
            for player_id in team1:
                user_doc = self.db_manager.find_one('users', {'discordid': str(player_id)})
                if user_doc and 'minecraft_uuid' in user_doc:
                    team1_data.append({'uuid': user_doc['minecraft_uuid'], 'ign': user_doc['ign']})

            team2_data = []
            for player_id in team2:
                user_doc = self.db_manager.find_one('users', {'discordid': str(player_id)})
                if user_doc and 'minecraft_uuid' in user_doc:
                    team2_data.append({'uuid': user_doc['minecraft_uuid'], 'ign': user_doc['ign']})

            if len(team1_data) != len(team1) or len(team2_data) != len(team2):
                logging.warning(f"Could not find UUIDs for all players in game {game_id}. Aborting WebSocket START_GAME message.")
                return

            start_game_message = {
                "type": "START_GAME",
                "game_id": game_id,
                "game_mode": queue_settings.get('gametype', 'unknown'),
                "teams": {
                    "team1": team1_data,
                    "team2": team2_data
                }
            }
            await self.ws_manager.broadcast(start_game_message)
            logging.info(f"Sent START_GAME event for game {game_id} via WebSocket.")

        except Exception as e:
            logging.error(f"Failed to send START_GAME WebSocket event for game {game_id}: {e}", exc_info=True)

    async def _start_game_batch(self, channel_id: str, batch: List[int], queue_settings: dict):
        try:
            logging.info(f"Starting game for batch of {len(batch)} players from queue {channel_id}")
            
            
            parties = []
            processed_players = set()
            
            for player_id in batch:
                if player_id not in processed_players:
                    party = self.party_manager.get_party_by_member(str(player_id))
                    if party:
                        party_members = set(int(member_id) for member_id in party['members']) & set(batch)
                        if party_members:
                            parties.append(party_members)
                            processed_players.update(party_members)
              
            teams = self.create_fair_teams(batch, parties)
            if not teams:
                logging.error(f"Failed to create teams for batch in queue {channel_id}")
                return
            
            team1, team2 = teams
              
            def generate_random_game_id():
                return ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
                
            
            game_id = generate_random_game_id()
            while self.db_manager.find_one('games', {'gameid': game_id}):
                
                game_id = generate_random_game_id()
                logging.debug(f"Generated new game ID due to collision: {game_id}")
            
            
            await self._send_start_game_websocket_event(game_id, queue_settings, team1, team2)


            game_text_channel = await self.create_game_channels(game_id, team1, team2)
            if not game_text_channel:
                logging.error(f"Failed to create game channels for game {game_id}")
                return
            
            
            game_channels = self.db_manager.find_one('gameschannels', {'textchannelid': str(game_text_channel.id)})
            if game_channels:
                await self.warp_players_to_channels(
                    team1, team2, 
                    game_channels['team1voicechannelid'], 
                    game_channels['team2voicechannelid']
                )
              
            team1_igns = await self.get_team_igns(team1)
            team2_igns = await self.get_team_igns(team2)
            
            
            gametype = 'casual' if queue_settings.get('iscasual') else 'ranked'
            
            
            current_timestamp = Timestamp(int(time.time()), 1)
                
            game_data = {
                'gameid': game_id,
                'team1': [str(player_id) for player_id in team1],  
                'team2': [str(player_id) for player_id in team2],  
                'state': 'pending',
                'gametype': queue_settings.get('gametype', 'unknown'),
                'date': current_timestamp,  
                'start_time': current_timestamp,  
                'end_time': current_timestamp  
            }
            self.db_manager.insert('games', game_data)
            
            
            for player_id in team1 + team2:
                try:
                    recent_game_auto_id = self.db_manager.get_next_sequence('recentgames')
                    self.db_manager.insert('recentgames', {
                        'id': str(recent_game_auto_id),
                        'discordid': str(player_id),
                        'gameid': game_id,
                        'result': 'pending',
                        'state': 'pending',
                        'ismvp': False,
                        'gametype': gametype,
                        'elochange': 0,
                        'date': current_timestamp
                    })
                    
                    
                    self.db_manager.increment(
                        'users', 
                        {'discordid': str(player_id)}, 
                        {'$inc': {'gamesplayed': 1}}
                    )
                except Exception as e:
                    logging.error(f"Error adding recent game for player {player_id}: {e}")
            
            
            if game_channels:
                self.db_manager.insert('gameschannels', {
                    'gameid': game_id,
                    'textchannelid': str(game_text_channel.id),
                    'team1voicechannelid': str(game_channels['team1voicechannelid']),
                    'team2voicechannelid': str(game_channels['team2voicechannelid'])
                })
            
            
            try:
                
                await self.send_teams_embed(game_text_channel, team1, team2, None, game_id)
                await self.send_seasoninfo_embed(game_text_channel)
                
                await self.send_party_invites(game_text_channel, team1_igns, team2_igns)
                
                gameschannelid = int(self.bot.config['channels']['games'])
                gameschanel = self.bot.get_channel(gameschannelid)
                await self.send_teams_embed(gameschanel, team1, team2, None, game_id)
            except Exception as e:
                logging.error(f"Error sending game details: {e}")
                
                await self.send_party_invites(game_text_channel, team1_igns, team2_igns)
            
            logging.info(f"Successfully created game {game_id} for queue {channel_id}")
        
        except Exception as e:
            logging.error(f"Error in _start_game_batch: {e}", exc_info=True)
        
        finally:
            
            for player_id in batch:
                self.players_in_game_creation.discard(player_id)
            self.release_player_locks(batch)

    async def move_players_to_voice_channels(self, team1: List[int], team2: List[int], game_text_channel: discord.TextChannel) -> None:
        pass  

    async def warp_players_to_channels(self, team1: list, team2: list, team1_channel_id: str, team2_channel_id: str):
        try:
            
            if self.websocket_enabled and self.ws_manager and hasattr(self.ws_manager, 'game_handler'):
                
                team1_igns = await self.get_team_igns(team1)
                team2_igns = await self.get_team_igns(team2)
                
                team1_status = await self.check_team_online_status(team1_igns)
                team2_status = await self.check_team_online_status(team2_igns)
                
                logging.info(f"Team 1 online status: {team1_status}")
                logging.info(f"Team 2 online status: {team2_status}")
                
                team1_online = []
                team2_online = []
                
                for ign in team1_igns:
                    if team1_status.get(ign, False):
                        discord_id = None
                        for player_id in team1:
                            user = self.db_manager.find_one('users', {'discordid': str(player_id), 'ign': ign})
                            if user:
                                discord_id = str(player_id)
                                break
                                
                        if discord_id:
                            
                            uuid = user.get('uuid', '')
                            team1_online.append({'ign': ign, 'uuid': uuid})
                
                for ign in team2_igns:
                    if team2_status.get(ign, False):
                        
                        discord_id = None
                        for player_id in team2:
                            user = self.db_manager.find_one('users', {'discordid': str(player_id), 'ign': ign})
                            if user:
                                discord_id = str(player_id)
                                break
                                
                        if discord_id:
                            
                            uuid = user.get('uuid', '')
                            team2_online.append({'ign': ign, 'uuid': uuid})
                
                
                if team1_online and team2_online:
                    try:
                        
                        game_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
                        map_name = "random"  
                        is_ranked = True  
                        
                        
                        success = await self.ws_manager.game_handler.warp_players(
                            game_id=game_id,
                            map_name=map_name,
                            is_ranked=is_ranked,
                            team1=team1_online,
                            team2=team2_online
                        )
                        
                        if success:
                            logging.info(f"Successfully warped players to game {game_id} using WebSocket")
                        else:
                            logging.warning(f"Failed to warp players to game {game_id} using WebSocket")
                    except Exception as e:
                        logging.error(f"Error warping players using WebSocket: {e}", exc_info=True)
            
            
            team1_channel = self.bot.get_channel(int(team1_channel_id))
            team2_channel = self.bot.get_channel(int(team2_channel_id))

            if not team1_channel or not team2_channel:
                print("One or both team channels are missing.")
                return

            moves = []
            for player_id in team1:
                moves.append({'player_id': player_id, 'channel_id': int(team1_channel_id)})
            for player_id in team2:
                moves.append({'player_id': player_id, 'channel_id': int(team2_channel_id)})

            if hasattr(self, 'worker_manager') and self.worker_manager.enabled:
                
                await self.worker_manager.move_players(moves)
            else:
                
                guild = self.bot.get_guild(self.guild_id)
                for move in moves:
                    member = guild.get_member(move['player_id'])
                    channel = self.bot.get_channel(move['channel_id'])
                    if member and channel:
                        try:
                            await member.move_to(channel)
                        except Exception as e:
                            print(f"Error moving player {move['player_id']} to channel {move['channel_id']}: {e}")
            print("Players have been warped to their respective channels.")
        except Exception as e:
            print(f"Error warping players to channels: {e}")

    def create_fair_teams(self, players: List[int], parties: List[Set[int]]) -> Optional[tuple[List[int], List[int]]]:
        try:
            team1, team2 = [], []
            remaining_players = set(players)
            queue_parties = []
            
            
            for party in parties:
                queue_parties.append({
                    'members': list(party),
                    'size': len(party)
                })
            queue_parties.sort(key=lambda x: x['size'], reverse=True)
            
            
            if queue_parties and len(queue_parties[0]['members']) <= len(players) // 2:
                team1.extend(queue_parties[0]['members'])
                remaining_players -= set(queue_parties[0]['members'])
                queue_parties.pop(0)
            
            
            for party in queue_parties[:]:
                if len(team2) + party['size'] <= len(players) // 2:
                    team2.extend(party['members'])
                    remaining_players -= set(party['members'])
                    queue_parties.remove(party)
            
            
            remaining = list(remaining_players)
            random.shuffle(remaining)
            
            
            while len(team1) < len(players) // 2 and remaining:
                team1.append(remaining.pop())
            
            
            team2.extend(remaining)
            
            return team1, team2
        except Exception as e:
            print(f"Error creating fair teams: {e}")
            return None

    async def create_game_channels(self, game_id: str, team1: List[int], team2: List[int]) -> Optional[discord.TextChannel]:
        try:
            print(f"Creating game channels for game {game_id}")

            guild_id = int(self.bot.config['bot']['guildid'])
            guild = self.bot.get_guild(guild_id)
            if not guild:
                print(f"Guild with ID {guild_id} not found")
                return None

            games_category = discord.utils.get(guild.categories, id=int(self.bot.config['categories']['gamestextcategory']))
            voice_category = discord.utils.get(guild.categories, id=int(self.bot.config['categories']['gamesvoicecategory']))

            if not games_category or not voice_category:
                print("Required categories not found")
                return None

            overwrites = {
                guild.default_role: discord.PermissionOverwrite(view_channel=False)
            }
            overwritesforvc = {
                guild.default_role: discord.PermissionOverwrite(view_channel=True)
            }

            text_channel_task = guild.create_text_channel(
                f"GAME #{game_id}", 
                category=games_category,
                overwrites=overwrites
            )

            voice_team1_task = guild.create_voice_channel(
                f"GAME #{game_id} T-1", 
                category=voice_category,
                overwrites=overwritesforvc
            )

            voice_team2_task = guild.create_voice_channel(
                f"GAME #{game_id} T-2", 
                category=voice_category,
                overwrites=overwritesforvc
            )

            text_channel, voice_team1, voice_team2 = await asyncio.gather(
                text_channel_task, voice_team1_task, voice_team2_task
            )

            print(f"Channels created: Text - {text_channel.id}, Team1 Voice - {voice_team1.id}, Team2 Voice - {voice_team2.id}")

            
            await text_channel.set_permissions(guild.default_role, view_channel=False, send_messages=False)
            
            await voice_team1.set_permissions(guild.default_role, view_channel=True, connect=False, speak=False)
            await voice_team2.set_permissions(guild.default_role, view_channel=True, connect=False, speak=False)

            for player_id in team1:
                member = guild.get_member(player_id)
                if member:
                    is_muted = await self.mute_manager.is_muted(player_id)
                    await text_channel.set_permissions(member, view_channel=True, send_messages=True)
                    await voice_team1.set_permissions(member, view_channel=True, connect=True, speak=not is_muted)

            for player_id in team2:
                member = guild.get_member(player_id)
                if member:
                    is_muted = await self.mute_manager.is_muted(player_id)
                    await text_channel.set_permissions(member, view_channel=True, send_messages=True)
                    await voice_team2.set_permissions(member, view_channel=True, connect=True, speak=not is_muted)

            game_channels_id = self.db_manager.get_next_sequence('gameschannels')
            self.db_manager.insert('gameschannels', {
                '_id': str(game_channels_id),
                'gameid': game_id,
                'textchannelid': str(text_channel.id),
                'team1voicechannelid': str(voice_team1.id),
                'team2voicechannelid': str(voice_team2.id)
            })

            return text_channel

        except Exception as e:
            print(f"Error creating game channels: {e}")
            return None

    async def get_team_igns(self, team: List[int]) -> List[str]:
        igns = []
        for player_id in team:
            user = self.db_manager.find_one('users', {'discordid': str(player_id)})
            if user and 'ign' in user:
                igns.append(user['ign'])
        return igns
        
    async def check_team_online_status(self, team_igns: List[str]) -> Dict[str, bool]:
        if not self.websocket_enabled or not self.ws_manager:
            
            return {ign: True for ign in team_igns}
            
        online_status = {}
        for ign in team_igns:
            online_status[ign] = await self.check_player_online(ign)
            
        return online_status

    async def send_teams_embed(self, channel: discord.TextChannel, team1: List[int], team2: List[int], _unused, game_id: str) -> None:
        try:
            team1_mentions = '\n'.join(f'- <@{player_id}>' for player_id in team1)
            team2_mentions = '\n'.join(f'- <@{player_id}>' for player_id in team2)

            embed = self.embed_builder.build_info(
                title=f"Game {game_id}",
                description=(
                    f"**Team 1**\n{team1_mentions}\n\n"
                    f"**Team 2**\n{team2_mentions}"
                )
            )
            await channel.send( embed=embed)

        except Exception as e:
            print(f"Error sending teams embed: {e}")
            try:
                await channel.send(embed=embed)
            except Exception as e2:
                print(f"Error in fallback send: {e2}")

    async def send_party_invites(self, channel: discord.TextChannel, team1_igns: List[str], team2_igns: List[str]) -> None:
        try:
            party_cmd = f"/p invite {' '.join(team1_igns + team2_igns)}"

            embed = self.embed_builder.build_info(
                title="Party Command",
                description=f"```{party_cmd}```"
            )
            await channel.send(embed=embed)

        except Exception as e:
            print(f"Error sending party invites: {e}")

    async def send_seasoninfo_embed(self, channel: discord.TextChannel) -> None:
        try:
            config_path = 'configs/seasoninfo.yml'
            
            if not os.path.exists(config_path):
                print(f"Season info file not found at {config_path}")
                return
            
            with open(config_path, 'r', encoding='utf-8') as file:
                content = file.read()
            
            content = content.replace(':rbw_yes:', '✅')
            content = content.replace(':rbw_maybe:', '⚠️')
            content = content.replace(':rbw_no:', '❌')
            
            lines = content.strip().split('\n')
            title = lines[0]
            description = '\n'.join(lines[1:])
            
            embed = self.embed_builder.build_info(
                title=title,
                description=description
            )
            
            await channel.send(embed=embed)
            
        except Exception as e:
            print(f"Error sending season info embed: {e}")

    def _init_continuous_processing(self):
        asyncio.create_task(self._load_and_start_continuous_processing())
    
    async def _load_and_start_continuous_processing(self):
        try:
            
            await self.bot.wait_until_ready()
              
            queues = self.db_manager.find('queues', {})
            
            for queue in queues:
                channel_id = queue['channelid']
                
                if channel_id not in self.queues:
                    self.queues[channel_id] = {
                        'players': set(),
                        'max_players': queue['maxplayers'],
                        'selected_map': None,
                        'parties': [],
                        'was_full': False,
                        'last_processed': 0,
                        'last_partial_check': 0
                    }
                
                
                await self.start_continuous_processing(channel_id)
            
            logging.info(f"Started continuous processing for {len(queues)} queues")
        
        except Exception as e:
            logging.error(f"Error initializing continuous processing: {e}", exc_info=True)
    
    async def start_continuous_processing(self, channel_id: str):
        if channel_id in self.continuous_queue_tasks and not self.continuous_queue_tasks[channel_id].done():
            
            return
        
        
        self.continuous_queue_tasks[channel_id] = asyncio.create_task(
            self._continuous_queue_processing(channel_id)
        )
        logging.info(f"Started continuous queue processing for channel {channel_id}")
    
    async def _continuous_queue_processing(self, channel_id: str):
        try:
            while True:
                await self.process_queue(channel_id, allow_partial=True)
                await asyncio.sleep(self.continuous_check_interval)
        except asyncio.CancelledError:
            logging.info(f"Continuous processing for queue {channel_id} was cancelled")
        except Exception as e:
            logging.error(f"Error in continuous queue processing for {channel_id}: {e}", exc_info=True)
            
            await asyncio.sleep(10)
            await self.start_continuous_processing(channel_id)
    async def process_queue(self, channel_id: str, allow_partial: bool = False):
        try:
            
            queue_settings = self.db_manager.find_one('queues', {'channelid': str(channel_id)})
            if not queue_settings or channel_id not in self.queues:
                return
            
            
            async with self.queue_locks.get(channel_id, asyncio.Lock()):
                queue = self.queues[channel_id]
                max_players = queue['max_players']
                
                
                available_players = list(queue['players'])
                if not available_players:
                    return
                
                
                all_parties = []
                for party_data in queue['parties']:
                    party_members = set(party_data['members'])
                    
                    if party_members.issubset(queue['players']):
                        all_parties.append(party_members)
                
                
                batches = []
                remaining = set(available_players)
                
                
                while len(remaining) >= max_players:
                    
                    batch, used_parties = self._create_batch(remaining, all_parties, max_players)
                    
                    if len(batch) == max_players:
                        
                        batches.append(batch)
                        remaining -= set(batch)
                        
                        
                        all_parties = [p for p in all_parties if p not in used_parties]
                    else:
                        
                        break
                
                
                if allow_partial and remaining and self.should_process_partial_batch(channel_id):
                    
                    queue['last_partial_check'] = time.time()
                    
                    
                    partial_batch, used_parties = self._create_batch(remaining, all_parties, len(remaining))
                    
                    if len(partial_batch) >= self.min_players_for_partial_game:
                        batches.append(partial_batch)
                        remaining -= set(partial_batch)
                        
                        
                        logging.info(f"Processing partial batch of {len(partial_batch)} players for queue {channel_id}")
                
                
                for batch in batches:
                    batch_list = list(batch)
                    
                    
                    if await self.acquire_player_locks(batch_list):
                        try:
                            
                            self.players_in_game_creation.update(batch_list)
                            
                            
                            for player_id in batch_list:
                                queue['players'].discard(player_id)
                                self.player_queue_map.pop(player_id, None)
                            
                            
                            asyncio.create_task(self._start_game_batch(
                                channel_id, batch_list, queue_settings
                            ))
                        except Exception as e:
                            
                            logging.error(f"Error starting game batch: {e}", exc_info=True)
                            for player_id in batch_list:
                                if player_id not in self.player_queue_map:
                                    queue['players'].add(player_id)
                                    self.player_queue_map[player_id] = channel_id
                            
                            
                            self.players_in_game_creation.difference_update(batch_list)
                            self.release_player_locks(batch_list)
                    else:
                        logging.warning(f"Failed to acquire locks for batch in queue {channel_id}")
        
        except Exception as e:
            logging.error(f"Error processing queue: {e}", exc_info=True)

    async def cleanup(self):
        try:
            
            for channel_id, task in self.continuous_queue_tasks.items():
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            
            
            for channel_id, task in self.queue_tasks.items():
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            
            logging.info("Queue processor cleaned up successfully")
        except Exception as e:
            logging.error(f"Error cleaning up queue processor: {e}", exc_info=True)
    
    async def restart_continuous_processing(self):
        try:
            
            for channel_id, task in self.continuous_queue_tasks.items():
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            
            
            self.continuous_queue_tasks.clear()
            
            
            await self._load_and_start_continuous_processing()
            
            logging.info("Restarted all continuous queue processing tasks")
        except Exception as e:
            logging.error(f"Error restarting continuous queue processing: {e}", exc_info=True)
    
    async def retry_game(self, interaction: discord.Interaction, game_id: str):
        user_roles = [role.id for role in interaction.user.roles]
        if not self.bot.permission_manager.has_permission('retry', user_roles):
            embed = discord.Embed(
                title='Permission Denied',
                description='You do not have permission to use this command.',
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        try:
            game = self.db_manager.find_one("games", {"gameid": game_id})
            if not game:
                await interaction.response.send_message("Game not found.", ephemeral=True)
                return

            
            team1 = [int(player_id) for player_id in game.get("team1", [])]
            team2 = [int(player_id) for player_id in game.get("team2", [])]

            
            team1_igns = await self.get_team_igns(team1)
            team2_igns = await self.get_team_igns(team2)

            
            game_channels = self.db_manager.find_one('gameschannels', {'gameid': game_id})
            if not game_channels:
                await interaction.response.send_message("Game channels not found.", ephemeral=True)
                return

            game_text_channel = self.bot.get_channel(int(game_channels['textchannelid']))
            if not game_text_channel:
                await interaction.response.send_message("Game text channel not found.", ephemeral=True)
                return

            
            await self.send_teams_embed(game_text_channel, team1, team2, None, game_id)
            await self.send_seasoninfo_embed(game_text_channel)
            await self.send_party_invites(game_text_channel, team1_igns, team2_igns)

            
            gameschannelid = int(self.bot.config['channels']['games'])
            gameschanel = self.bot.get_channel(gameschannelid)
            if gameschanel:
                await self.send_teams_embed(gameschanel, team1, team2, None, game_id)

            embed = discord.Embed(
                title="Game Retry Initiated",
                description=f"Game {game_id} is being retried.",
                color=discord.Color.green()
            )
            await interaction.response.send_message(embed=embed, ephemeral=False)

        except Exception as e:
            embed = discord.Embed(
                title="Error",
                description=f"An error occurred while retrying the game: {str(e)}",
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
