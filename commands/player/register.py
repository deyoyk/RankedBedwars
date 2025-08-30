import discord
from discord import app_commands
from discord.ext import commands
from discord import ui
from typing import Optional, Dict, Any, Union, Callable
import yaml
import os
import random
import string
import asyncio
import aiohttp

from managers.permission_manager import PermissionManager
from utils.embed_builder import EmbedBuilder
from utils.error_handler import ErrorHandler
from actions.fix import fix


class VerificationModal(ui.Modal, title="Verify Minecraft Account"):
    verification_code = ui.TextInput(
        label="Verification Code",
        placeholder="Enter the code you received in-game",
        min_length=6,
        max_length=6,
        required=True
    )
    
    def __init__(self, callback: Callable):
        super().__init__()
        self.callback = callback
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        await self.callback(interaction, self.verification_code.value)


class VerificationView(ui.View):
    def __init__(self, verify_callback: Callable, timeout: int = 600):
        super().__init__(timeout=timeout)
        self.verify_callback = verify_callback
    
    @ui.button(label="Enter Verification Code", style=discord.ButtonStyle.primary)
    async def verify_button(self, interaction: discord.Interaction, button: ui.Button):
        modal = VerificationModal(self.verify_callback)
        await interaction.response.send_modal(modal)


class RegisterCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.permission_manager = PermissionManager()
        self.embed_builder = EmbedBuilder()
        self.error_handler = ErrorHandler(bot)
        self.config = self.load_config()
        
        
        self.websocket_enabled = self.config.get('websocket', {}).get('enabled', False)
        self.ws_manager = getattr(bot, 'websocket_manager', None) if self.websocket_enabled else None
        
        
        self.pending_verifications = {}  

    def load_config(self) -> Dict[str, Any]:
        config_path = os.path.join('configs', 'config.yml')
        try:
            with open(config_path, 'r', encoding='utf-8') as file:
                return yaml.safe_load(file)
        except Exception as e:
            self.bot.logger.error(f"Failed to load configuration: {e}")
            raise
    
    def generate_verification_code(self) -> str:
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))

    async def get_minecraft_uuid(self, username: str) -> Optional[str]:
        url = f"https://api.mojang.com/users/profiles/minecraft/{username}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get("id")
                    else:
                        return None
        except Exception as e:
            self.bot.logger.error(f"Error fetching Minecraft UUID for {username}: {e}")
            return None

    async def check_player_online(self, ign: str) -> bool:
        if not self.websocket_enabled or not self.ws_manager:
            return False
            
        try:
            player_handler = getattr(self.ws_manager, 'player_handler', None)
            if player_handler:
                return await player_handler.check_player_online(ign, timeout=10.0)
            return False
        except Exception as e:
            self.bot.logger.error(f"Error checking if player {ign} is online: {e}")
            return False
    
    async def send_verification_code(self, ign: str, code: str) -> bool:
        if not self.websocket_enabled or not self.ws_manager:
            return False
            
        try:
            
            
            verification_message = {
                'type': 'verification_code',
                'ign': ign,
                'code': code,
                'message': f"Your Discord verification code is: {code}"
            }
            
            
            if hasattr(self.ws_manager, 'broadcast'):
                await self.ws_manager.broadcast(verification_message)
                self.bot.logger.info(f"Sent verification code to {ign}: {code}")
                return True
            return False
        except Exception as e:
            self.bot.logger.error(f"Error sending verification code to {ign}: {e}")
            return False
    
    async def handle_verification_code_submit(self, interaction: discord.Interaction, code: str) -> None:
        user_id = str(interaction.user.id)
        
        if user_id not in self.pending_verifications:
            await interaction.followup.send(
                embed=self.embed_builder.build_error(
                    title="Verification Failed",
                    description="No verification pending. Please start registration again."
                ),
                ephemeral=True
            )
            return
            
        verification = self.pending_verifications[user_id]
        
        
        if verification.get("expires", 0) < discord.utils.utcnow().timestamp():
            del self.pending_verifications[user_id]
            await interaction.followup.send(
                embed=self.embed_builder.build_error(
                    title="Verification Expired",
                    description="Your verification code has expired. Please start registration again."
                ),
                ephemeral=True
            )
            return
            
        
        if verification["code"] != code:
            await interaction.followup.send(
                embed=self.embed_builder.build_error(
                    title="Invalid Code",
                    description="The verification code you entered is incorrect. Please try again."
                ),
                ephemeral=True
            )
            return
            
        
        ign = verification["ign"]
        uuid = verification["uuid"]
        await self.register_player(interaction, ign, uuid, verified=True)
        
        
        del self.pending_verifications[user_id]

    async def start_websocket_verification(self, interaction_or_ctx, ign: str, uuid: str, is_prefix: bool = False) -> bool:
        user = interaction_or_ctx.user if not is_prefix else interaction_or_ctx.author
        user_id = str(user.id)
        
        
        if not self.websocket_enabled or not self.ws_manager:
            return True
            
        
        is_online = await self.check_player_online(ign)
        if not is_online:
            error_embed = self.embed_builder.build_error(
                title="Player Not Online",
                description=f"The player `{ign}` is not currently online on the server. Please join the server and try again."
            )
            
            if is_prefix:
                await interaction_or_ctx.reply(embed=error_embed)
            else:
                if interaction_or_ctx.response.is_done():
                    await interaction_or_ctx.followup.send(embed=error_embed, ephemeral=True)
                else:
                    await interaction_or_ctx.response.send_message(embed=error_embed, ephemeral=True)
            return False
            
        
        verification_code = self.generate_verification_code()
        
        
        expiration = discord.utils.utcnow().timestamp() + 300  
        self.pending_verifications[user_id] = {
            "code": verification_code,
            "ign": ign,
            "uuid": uuid,
            "expires": expiration
        }
        
        
        sent = await self.send_verification_code(ign, verification_code)
        if not sent:
            error_embed = self.embed_builder.build_error(
                title="Verification Failed",
                description="Could not send verification code to the player. Please try again later."
            )
            
            if is_prefix:
                await interaction_or_ctx.reply(embed=error_embed)
            else:
                if interaction_or_ctx.response.is_done():
                    await interaction_or_ctx.followup.send(embed=error_embed, ephemeral=True)
                else:
                    await interaction_or_ctx.response.send_message(embed=error_embed, ephemeral=True)
            
            
            del self.pending_verifications[user_id]
            return False
        
        
        verify_embed = self.embed_builder.build_info(
            title="Verification Required",
            description=(
                f"A verification code has been sent to `{ign}` in-game.\n\n"
                f"Please check your in-game chat and click the button below to enter the code.\n\n"
                f"This code will expire in 5 minutes."
            )
        )
        
        
        view = VerificationView(self.handle_verification_code_submit)
        
        if is_prefix:
            await interaction_or_ctx.reply(embed=verify_embed, view=view)
        else:
            if interaction_or_ctx.response.is_done():
                await interaction_or_ctx.followup.send(embed=verify_embed, view=view, ephemeral=True)
            else:
                await interaction_or_ctx.response.send_message(embed=verify_embed, view=view, ephemeral=True)
        
        return False  
        
    async def register_player(self, interaction_or_ctx, ign: str, uuid: str, is_prefix: bool = False, verified: bool = False) -> None:
        try:
            user = interaction_or_ctx.user if not is_prefix else interaction_or_ctx.author

            
            if not verified and self.websocket_enabled and self.ws_manager:
                started = await self.start_websocket_verification(interaction_or_ctx, ign, uuid, is_prefix)
                if not started:
                    return  
            
            
            if is_prefix:
                msg = await interaction_or_ctx.reply(embed=self.embed_builder.build_info(
                    title='Processing Registration',
                    description='Please wait while we register your account...'
                ))
            else:
                if not interaction_or_ctx.response.is_done():
                    await interaction_or_ctx.response.send_message(
                        embed=self.embed_builder.build_info(
                            title='Processing Registration',
                            description='Please wait while we register your account...'
                        ),
                        ephemeral=True
                    )

            document = {
                'discordid': str(user.id),
                'ign': ign,
                'minecraft_uuid': uuid,
                'exp': 0,
                'totalexp': 0,
                'level': 1,
                'elo': 0,
                'wins': 0,
                'losses': 0,
                'kills': 0,
                'deaths': 0,
                'winstreak': 0,
                'loosestreak': 0,
                'highest_elo': 0,
                'highstwinstreak': 0,
                'bedsbroken': 0,
                'mvps': 0,
                'ss': 0,
                'scored': 0,
                'voided': 0,
                'gamesplayed': 0,
            }

            self.bot.database_manager.insert('users', document)
            self.bot.database_manager.insert('settings', {
                'discordid': str(user.id),
                'isprefixtoggled': False,
                'ispartyinvitestoggled': False,
                'isscoringpingtoggled': False,
                'staticnickname': False,
                'nickname': '',
                'theme': 'default',
                'skinpose': 'default'
            })

            await fix(self.bot, user.id, interaction_or_ctx.guild.id)

            success_embed = self.embed_builder.build_success(
                title='Registration Complete',
                description=f'Welcome to Ranked Bedwars, {user.mention}! Your registration is now complete.'
            )

            if is_prefix:
                await msg.edit(embed=success_embed)
            else:
                if interaction_or_ctx.response.is_done():
                    await interaction_or_ctx.edit_original_response(embed=success_embed)
                else:
                    await interaction_or_ctx.response.send_message(embed=success_embed, ephemeral=True)


            
            try:
                config = self.config
                reg_log_channel_id = int(config.get('logging', {}).get('regandrename'))
                reg_log_channel = self.bot.get_channel(reg_log_channel_id)
                if reg_log_channel:
                    log_embed = discord.Embed(
                        title="User Registration",
                        color=discord.Color.green()
                    )
                    log_embed.add_field(name="User", value=f"<@{user.id}> ({user.id})", inline=True)
                    log_embed.add_field(name="IGN", value=f"`{ign}`", inline=True)
                    log_embed.add_field(name="UUID", value=f"`{uuid}`", inline=True)
                    log_embed.add_field(name="Method", value="Prefix Command" if is_prefix else "Slash Command", inline=True)
                    log_embed.add_field(name="Registered By", value=f"<@{user.id}> ({user.id})", inline=True)
                    log_embed.set_footer(text=f"Guild ID: {interaction_or_ctx.guild.id if hasattr(interaction_or_ctx, 'guild') and interaction_or_ctx.guild else 'N/A'}")
                    await reg_log_channel.send(embed=log_embed)
            except Exception as log_exc:
                print(f"Failed to send regandrename log: {log_exc}")

        except Exception as e:
            await self.error_handler.handle_error(e, 'player registration')

            error_embed = self.embed_builder.build_error(
                description='An error occurred while registering. Please try again later.'
            )

            if is_prefix:
                await interaction_or_ctx.reply(embed=error_embed)
            else:
                if interaction_or_ctx.response.is_done():
                    await interaction_or_ctx.edit_original_response(embed=error_embed)
                else:
                    await interaction_or_ctx.response.send_message(embed=error_embed, ephemeral=True)

    
    @app_commands.command(name='register', description='Register your Minecraft IGN with the bot')
    @app_commands.describe(ign='Your Minecraft in-game name (IGN)')
    async def register(self, interaction: discord.Interaction, ign: str):
        user_roles = [role.id for role in interaction.user.roles]
        if not self.permission_manager.has_permission('register', user_roles):
            embed = self.embed_builder.build_error(
                title='Permission Denied',
                description='You do not have permission to use this command.'
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        try:
            
            uuid = await self.get_minecraft_uuid(ign)
            if not uuid:
                await interaction.response.send_message(
                    embed=self.embed_builder.build_error(
                        title="Invalid IGN",
                        description=f"The Minecraft username `{ign}` does not exist. Please check the spelling."
                    ),
                    ephemeral=True
                )
                return


            query = {'discordid': str(interaction.user.id)}
            player = self.bot.database_manager.find_one('users', query)

            if player:
                await interaction.response.send_message(
                    embed=self.embed_builder.build_warning(
                        description=f'{interaction.user.mention}, you are already registered!'
                    ),
                    ephemeral=True
                )
                return

            
            existing_ign = self.bot.database_manager.find_one('users', {'ign': {'$regex': f'^{ign}$', '$options': 'i'}})
            if existing_ign:
                await interaction.response.send_message(
                    embed=self.embed_builder.build_error(
                        title='IGN Taken',
                        description=f'The IGN `{ign}` is already taken by another user.'
                    ),
                    ephemeral=True
                )
                return
                
            
            await self.register_player(interaction, ign, uuid)

        except Exception as e:
            await self.error_handler.handle_error(e, 'player registration')
            await interaction.response.send_message(
                embed=self.embed_builder.build_error(
                    description='An error occurred while registering. Please try again later.'
                ),
                ephemeral=True
            )

    
    @commands.command(name="register")
    async def register_prefix(self, ctx: commands.Context, ign: str):
        user_roles = [role.id for role in ctx.author.roles]
        if not self.permission_manager.has_permission('register', user_roles):
            await ctx.reply(embed=self.embed_builder.build_error(
                title='Permission Denied',
                description='You do not have permission to use this command.'
            ))
            return

        try:
            
            uuid = await self.get_minecraft_uuid(ign)
            if not uuid:
                await ctx.reply(embed=self.embed_builder.build_error(
                    title="Invalid IGN",
                    description=f"The Minecraft username `{ign}` does not exist. Please check the spelling."
                ))
                return


            query = {'discordid': str(ctx.author.id)}
            player = self.bot.database_manager.find_one('users', query)

            if player:
                await ctx.reply(embed=self.embed_builder.build_warning(
                    description=f'{ctx.author.mention}, you are already registered!'
                ))
                return

            
            existing_ign = self.bot.database_manager.find_one('users', {'ign': {'$regex': f'^{ign}$', '$options': 'i'}})
            if existing_ign:
                await ctx.reply(embed=self.embed_builder.build_error(
                    title='IGN Taken',
                    description=f'The IGN `{ign}` is already taken by another user.'
                ))
                return
                
            
            await self.register_player(ctx, ign, uuid, is_prefix=True)

        except Exception as e:
            await self.error_handler.handle_error(e, 'prefix register')
            await ctx.reply(embed=self.embed_builder.build_error(
                description='An error occurred while registering. Please try again later.'
            ))


async def setup(bot):
    await bot.add_cog(RegisterCommands(bot))
