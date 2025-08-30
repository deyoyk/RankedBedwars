
import asyncio
import logging
from typing import Dict, Any, Optional, List
from ..models.messages import MessageType
from ..utils.error_handler import WebSocketErrorHandler
from managers.database_manager import DatabaseManager


class ScoringHandler:
    
    def __init__(self, websocket_manager):
        self.ws_manager = websocket_manager
        self.bot = websocket_manager.bot
        self.logger = websocket_manager.logger
        self.config = websocket_manager.config
        
        
        self._register_handlers()
        
        self.logger.info("ScoringHandler initialized successfully")
    
    def _register_handlers(self):
        self.ws_manager.register_handler(MessageType.SCORING, self.handle_scoring)
        self.ws_manager.register_handler(MessageType.VOIDING, self.handle_voiding)
        
        self.logger.debug("ScoringHandler message handlers registered")
    
    async def handle_scoring(self, message: Dict[str, Any], websocket) -> None:
        try:
            game_id = message.get('gameid')
            if not game_id:
                self.logger.error("Scoring message missing gameid")
                return
            
            self.logger.info(f"Processing scoring for game {game_id}")
            
            
            winning_team_number = message.get('winningTeamNumber')
            players_data = message.get('players', {})
            provided_mvps = message.get('mvps', [])
            provided_bedbreakers = message.get('bedsbroken', [])
            
            if winning_team_number is None:
                self.logger.error(f"Scoring message for game {game_id} missing winningTeamNumber")
                return
            
            
            processed_stats = self._process_player_stats(players_data)
            mvp_igns = self._determine_mvps(players_data, provided_mvps)

            # Determine bed breakers from stats if not provided
            if provided_bedbreakers:
                bedbreaker_igns = provided_bedbreakers
            else:
                bedbreaker_igns = [ign for ign, stats in players_data.items() if stats.get('bedbroke', False)]

            mvp_ids = await self._convert_igns_to_ids(mvp_igns)
            bedbreaker_ids = await self._convert_igns_to_ids(bedbreaker_igns)
            
            
            await self._call_existing_scoring(
                game_id=game_id,
                winning_team_number=winning_team_number,
                mvp_ids=mvp_ids,
                bedbreaker_ids=bedbreaker_ids,
                player_stats=processed_stats
            )
            
            self.logger.info(f"Successfully processed scoring for game {game_id}")
            
        except Exception as e:
            self.logger.error(f"Error handling scoring message: {e}")
            await self.ws_manager.error_handler.handle_message_error(websocket, str(message), e)
    
    async def handle_voiding(self, message: Dict[str, Any], websocket) -> None:
        try:
            game_id = message.get('gameid')
            reason = message.get('reason', 'No reason provided')
            
            if not game_id:
                self.logger.error("Voiding message missing gameid")
                return
            
            self.logger.info(f"Processing voiding for game {game_id}: {reason}")
            
            
            await self._call_existing_voiding(game_id, reason)
            
            self.logger.info(f"Successfully processed voiding for game {game_id}")
            
        except Exception as e:
            self.logger.error(f"Error handling voiding message: {e}")
            await self.ws_manager.error_handler.handle_message_error(websocket, str(message), e)
    
    def _process_player_stats(self, players_data: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        try:
            processed_stats = {}
            
            for ign, stats in players_data.items():
                
                processed_stats[ign] = {
                    'kills': max(0, int(stats.get('kills', 0))),
                    'deaths': max(0, int(stats.get('deaths', 0))),
                    'bedbroke': bool(stats.get('bedbroken', False)),
                    'finalkills': max(0, int(stats.get('finalkills', 0))),
                    'diamonds': max(0, int(stats.get('diamonds', 0))),
                    'irons': max(0, int(stats.get('irons', 0))),
                    'gold': max(0, int(stats.get('gold', 0))),
                    'emeralds': max(0, int(stats.get('emeralds', 0))),
                    'blocksplaced': max(0, int(stats.get('blocksplaced', 0)))
                }
            
            self.logger.debug(f"Processed stats for {len(processed_stats)} players")
            return processed_stats
            
        except Exception as e:
            self.logger.error(f"Error processing player stats: {e}")
            return {}
    
    def _determine_mvps(self, players_data: Dict[str, Dict[str, Any]], provided_mvps: List[str]) -> List[str]:
        try:
            if provided_mvps:
                self.logger.debug(f"Using provided MVPs: {provided_mvps}")
                return provided_mvps
            
            if not players_data:
                self.logger.warning("No player data available for MVP determination")
                return []

            # Calculate a score for each player (e.g., final kills are worth more)
            player_scores = {}
            for ign, stats in players_data.items():
                kills = int(stats.get('kills', 0))
                final_kills = int(stats.get('finalkills', 0))
                player_scores[ign] = kills + (2 * final_kills)

            if not player_scores:
                return []

            max_score = max(player_scores.values())
            
            if max_score == 0:
                self.logger.debug("No player scored any points for MVP consideration.")
                return []

            mvps = [ign for ign, score in player_scores.items() if score == max_score]
            
            self.logger.debug(f"Determined MVPs based on a max score of {max_score}: {mvps}")
            return mvps
            
        except Exception as e:
            self.logger.error(f"Error determining MVPs: {e}")
            return provided_mvps or []
    
    async def _convert_igns_to_ids(self, igns: List[str]) -> List[str]:
        try:
            if not igns:
                return []
            
            db_manager = DatabaseManager()
            discord_ids = []
            
            try:
                for ign in igns:
                    
                    user = db_manager.find_one('users', {'ign': ign})
                    if user and 'discordid' in user:
                        discord_ids.append(user['discordid'])
                    else:
                        self.logger.warning(f"Could not find Discord ID for IGN: {ign}")
                
                self.logger.debug(f"Converted {len(igns)} IGNs to {len(discord_ids)} Discord IDs")
                return discord_ids
                
            finally:
                db_manager.close()
                
        except Exception as e:
            self.logger.error(f"Error converting IGNs to Discord IDs: {e}")
            return []
    
    async def _call_existing_scoring(
        self, 
        game_id: str, 
        winning_team_number: int, 
        mvp_ids: List[str], 
        bedbreaker_ids: List[str], 
        player_stats: Dict[str, Dict[str, Any]]
    ) -> None:
        try:
            
            from actions.scoring import scoring
            
            self.logger.info(f"Calling existing scoring system for game {game_id}")
            
            
            await scoring(
                bot=self.bot,
                gameid=game_id,
                winningteamnumber=winning_team_number,
                mvp_ids=mvp_ids,
                bedbreaker_ids=bedbreaker_ids,
                player_stats=player_stats,
                iscasual=False,  
                scoredby=None    
            )
            
            self.logger.info(f"Successfully called existing scoring system for game {game_id}")
            
        except Exception as e:
            self.logger.error(f"Error calling existing scoring system for game {game_id}: {e}")
            raise e
    
    async def _call_existing_voiding(self, game_id: str, reason: str) -> None:
        try:
            
            from actions.voiding import void
            
            self.logger.info(f"Calling existing voiding system for game {game_id}: {reason}")
            
            
            await void(
                bot=self.bot,
                gameid=game_id,
                staffid=None  
            )
            
            self.logger.info(f"Successfully called existing voiding system for game {game_id}")
            
        except Exception as e:
            self.logger.error(f"Error calling existing voiding system for game {game_id}: {e}")
            raise e
    
    def get_scoring_stats(self) -> Dict[str, Any]:
        return {
            'handler_type': 'ScoringHandler',
            'registered_message_types': [MessageType.SCORING, MessageType.VOIDING],
            'status': 'active'
        }
    
    async def cleanup(self) -> None:
        try:
            self.logger.info("Cleaning up ScoringHandler...")
            
            
            
            
            self.logger.info("ScoringHandler cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Error during ScoringHandler cleanup: {e}")
