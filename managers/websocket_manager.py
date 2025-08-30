import asyncio
import json
import logging
import websockets
from typing import Set, Dict, Any, Optional, Callable
from websockets.server import WebSocketServerProtocol
from .websocket.utils.error_handler import WebSocketErrorHandler, MessageValidationError, HandlerNotFoundError
from .websocket.utils.callbacks import CallbackManager, RequestResponseHandler
from .websocket.handlers.player_handler import PlayerHandler


class WebSocketManager:
    
    def __init__(self, bot, config: dict):
        self.bot = bot
        self.config = config
        self.logger = bot.logger
        
        
        websocket_config = config.get('websocket', {})
        self.enabled = websocket_config.get('enabled', False)
        self.host = websocket_config.get('host', '0.0.0.0')
        self.port = websocket_config.get('port', 8080)
        self.path = websocket_config.get('path', '/rbw/websocket')
        self.auth_token = websocket_config.get('auth_token')
        self.timeout = websocket_config.get('timeout', 60)
        self.max_retry_attempts = websocket_config.get('max_retry_attempts', 3)
        self.queue_broadcast_interval = websocket_config.get('queue_broadcast_interval', 1.0)

        if self.enabled and not self.auth_token:
            self.logger.warning("WebSocket is enabled but no auth_token is set in config.yml. Connections will fail.")
        
        
        self.clients: Set[WebSocketServerProtocol] = set()
        self.server = None
        
        
        self.handlers: Dict[str, Any] = {}
        
        
        self.callback_manager = CallbackManager(default_timeout=self.timeout)
        self.request_response_handler = RequestResponseHandler(self.callback_manager)
        
        
        self.warp_timeouts: Dict[str, Any] = {}
        self.warp_attempts: Dict[str, int] = {}
        self.team_data: Dict[str, Any] = {}
        
        
        self._background_tasks: Set[asyncio.Task] = set()
        
        
        self.error_handler = WebSocketErrorHandler(self)
        
        
        self.start_time = None
        
        
        self._connection_health_task = None
        
        
        self.player_handler = None
        self.queue_handler = None
        self.game_handler = None
        self.scoring_handler = None
        self.voice_handler = None
        self.screenshare_handler = None
        if self.enabled:
            self._initialize_handlers()
        
        self.logger.info(f"WebSocket manager initialized - Enabled: {self.enabled}")
        if self.enabled:
            self.logger.info(f"WebSocket will listen on {self.host}:{self.port} at path {self.path}")
    
    async def start(self) -> None:
        if not self.enabled:
            self.logger.info("WebSocket system is disabled, skipping initialization")
            return
        
        try:
            self.logger.info(f"Starting WebSocket server on {self.host}:{self.port}")
            self.start_time = asyncio.get_event_loop().time()
            
            
            async def connection_handler(websocket):
                
                path = getattr(websocket, 'path', self.path)
                await self.handle_connection(websocket, path)
            
            
            self.server = await websockets.serve(
                connection_handler,
                self.host,
                self.port,
                logger=self.logger,
                ping_interval=30,  
                ping_timeout=10,   
                close_timeout=10   
            )
            
            
            await self.callback_manager.start()
            
            
            self._connection_health_task = asyncio.create_task(self._monitor_connection_health())
            self._background_tasks.add(self._connection_health_task)
            
            
            if self.queue_handler:
                await self.queue_handler.start_broadcasting()
            
            self.logger.info(f"WebSocket server started successfully on ws://{self.host}:{self.port}{self.path}")
            
        except Exception as e:
            await self.error_handler.handle_connection_error(None, e)
            await self.error_handler.handle_critical_error("WebSocket Server Startup", e, should_shutdown=True)
            self.logger.error(f"Failed to start WebSocket server: {e}")
            raise
    
    async def stop(self) -> None:
        if not self.enabled or not self.server:
            return
        
        try:
            self.logger.info("Stopping WebSocket server...")
            
            
            try:
                await self.callback_manager.stop()
            except Exception as e:
                await self.error_handler.handle_shutdown_error("Callback Manager", e)
            
            
            if self.queue_handler:
                try:
                    await self.queue_handler.stop_broadcasting()
                except Exception as e:
                    await self.error_handler.handle_shutdown_error("Queue Handler", e)
            
            
            for task in self._background_tasks:
                if not task.done():
                    task.cancel()
            
            
            if self._background_tasks:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*self._background_tasks, return_exceptions=True),
                        timeout=5.0
                    )
                except asyncio.TimeoutError:
                    await self.error_handler.handle_shutdown_error(
                        "Background Tasks", 
                        asyncio.TimeoutError("Background tasks did not complete within timeout"),
                        {"task_count": len(self._background_tasks)}
                    )
            
            
            self._background_tasks.clear()
            
            
            if self.clients:
                self.logger.info(f"Closing {len(self.clients)} client connections")
                close_tasks = []
                for client in self.clients.copy():
                    try:
                        close_tasks.append(client.close(code=1001, reason="Server shutting down"))
                    except Exception as e:
                        await self.error_handler.handle_shutdown_error("Client Connection Close", e, {"client": str(client)})
                
                if close_tasks:
                    results = await asyncio.gather(*close_tasks, return_exceptions=True)
                    for i, result in enumerate(results):
                        if isinstance(result, Exception):
                            await self.error_handler.handle_shutdown_error("Client Close Task", result)
            
            
            self.clients.clear()
            
            
            try:
                self.server.close()
                await self.server.wait_closed()
            except Exception as e:
                await self.error_handler.handle_shutdown_error("WebSocket Server", e)
            
            self.logger.info("WebSocket server stopped successfully")
            
        except Exception as e:
            await self.error_handler.handle_shutdown_error("WebSocket Manager Stop", e)
            self.logger.error(f"Error stopping WebSocket server: {e}")
    
    async def handle_connection(self, websocket: WebSocketServerProtocol, path: str) -> None:
        if not self.enabled:
            await websocket.close(code=1000, reason="WebSocket system disabled")
            return
        
        if path != self.path:
            self.logger.warning(f"Connection attempted with invalid path: {path}")
            await websocket.close(code=1000, reason="Invalid path")
            return

        client_address = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}" if websocket.remote_address else "unknown"

        try:
            auth_message_str = await asyncio.wait_for(websocket.recv(), timeout=10.0)
            auth_message = json.loads(auth_message_str)

            if auth_message.get("type") != "auth" or auth_message.get("token") != self.auth_token:
                self.logger.warning(f"WebSocket connection failed authentication from {client_address}")
                await websocket.close(code=1011, reason="Authentication failed")
                return

            self.logger.info(f"WebSocket client authenticated successfully from {client_address}")

        except asyncio.TimeoutError:
            self.logger.warning(f"WebSocket connection from {client_address} timed out waiting for authentication.")
            await websocket.close(code=1002, reason="Authentication timed out")
            return
        except (json.JSONDecodeError, websockets.exceptions.ConnectionClosed) as e:
            self.logger.warning(f"WebSocket connection failed during authentication phase from {client_address}: {e}")
            await websocket.close(code=1002, reason="Authentication failed")
            return
        except Exception as e:
            self.logger.error(f"Unexpected error during WebSocket authentication from {client_address}: {e}")
            await websocket.close(code=1011, reason="Internal server error during authentication")
            return

        self.clients.add(websocket)
        self.logger.info(f"New authenticated WebSocket connection from {client_address} added to client pool.")
        self.error_handler.track_connection_attempt(success=True)
        
        try:
            welcome_message = {
                "type": "welcome",
                "message": "Connection authenticated and established.",
                "server": "RBW Discord Bot WebSocket",
                "timestamp": asyncio.get_event_loop().time()
            }
            await self.send_to_client(websocket, welcome_message)
            
            async for message in websocket:
                await self.handle_message(websocket, message)
                
        except websockets.exceptions.ConnectionClosed as e:
            self.logger.info(f"WebSocket connection closed: {client_address} (code: {e.code}, reason: {e.reason})")
            self.error_handler.connection_health['disconnections'] += 1
        except websockets.exceptions.WebSocketException as e:
            self.error_handler.track_connection_attempt(success=False)
            await self.error_handler.handle_connection_error(websocket, e)
        except Exception as e:
            self.error_handler.track_connection_attempt(success=False)
            await self.error_handler.handle_connection_error(websocket, e)
        finally:
            self.clients.discard(websocket)
            self.logger.info(f"Removed client {client_address} from active connections")
    
    async def handle_message(self, websocket: WebSocketServerProtocol, data: str) -> None:
        if not self.enabled:
            return
        
        try:
            
            try:
                message = json.loads(data)
            except json.JSONDecodeError as e:
                await self.error_handler.handle_message_error(websocket, data, e)
                return
            
            message_type = message.get('type')
            
            if not message_type:
                error = MessageValidationError("Message missing required 'type' field")
                await self.error_handler.handle_message_error(websocket, data, error)
                return
            
            self.logger.debug(f"Received message type: {message_type}")
            
            
            if message_type == "ping":
                await self._handle_ping(websocket, message)
                return
            elif message_type == "pong":
                await self._handle_pong(websocket, message)
                return
            
            
            request_id = message.get('request_id')
            if request_id and self.callback_manager.get_request_info(request_id):
                
                await self._handle_callback_response(message)
                return
            
            
            if message_type in self.handlers:
                try:
                    await self.handlers[message_type](message, websocket)
                except Exception as handler_error:
                    await self.error_handler.handle_message_error(websocket, data, handler_error)
            else:
                error = HandlerNotFoundError(f"No handler found for message type: {message_type}")
                await self.error_handler.handle_message_error(websocket, data, error)
                
        except Exception as e:
            await self.error_handler.handle_message_error(websocket, data, e)
    
    async def broadcast(self, message: dict) -> None:
        if not self.enabled or not self.clients:
            return
        
        try:
            message_json = json.dumps(message)
            self.logger.debug(f"Broadcasting message to {len(self.clients)} clients: {message.get('type', 'unknown')}")
            
            
            disconnected_clients = set()
            send_tasks = []
            
            for client in self.clients.copy():
                try:
                    
                    task = asyncio.create_task(self._send_to_client_safe(client, message_json))
                    send_tasks.append((client, task))
                except Exception as e:
                    self.logger.debug(f"Error creating send task for client: {e}")
                    disconnected_clients.add(client)
            
            
            for client, task in send_tasks:
                try:
                    await task
                except Exception as e:
                    self.logger.debug(f"Error sending to client during broadcast: {e}")
                    disconnected_clients.add(client)
            
            
            for client in disconnected_clients:
                self.clients.discard(client)
                
        except Exception as e:
            self.error_handler.log_error("BROADCAST", f"Error broadcasting message: {e}", e)
    
    async def send_to_client(self, websocket: WebSocketServerProtocol, message: dict) -> None:
        if not self.enabled:
            return
        
        try:
            message_json = json.dumps(message)
            await websocket.send(message_json)
            self.logger.debug(f"Sent message to client: {message.get('type', 'unknown')}")
            
        except websockets.exceptions.ConnectionClosed:
            self.logger.debug("Attempted to send message to closed connection")
            self.clients.discard(websocket)
        except Exception as e:
            self.error_handler.log_error("SEND_CLIENT", f"Error sending message to client: {e}", e)
    
    async def _send_to_client_safe(self, websocket: WebSocketServerProtocol, message_json: str) -> None:
        try:
            await websocket.send(message_json)
        except websockets.exceptions.ConnectionClosed:
            raise  
        except Exception as e:
            self.logger.debug(f"Error in safe send to client: {e}")
            raise
    
    def is_enabled(self) -> bool:
        return self.enabled
    
    def get_client_count(self) -> int:
        return len(self.clients)
    
    def get_status(self) -> dict:
        status = {
            'enabled': self.enabled,
            'server_running': self.server is not None,
            'client_count': len(self.clients),
            'host': self.host,
            'port': self.port,
            'path': self.path,
            'background_tasks': len(self._background_tasks),
            'pending_requests': self.callback_manager.get_pending_count()
        }
        
        
        if self.queue_handler:
            status['queue_handler'] = self.queue_handler.get_queue_stats()
        
        
        if self.game_handler:
            status['game_handler'] = self.game_handler.get_warp_stats()
        
        
        if self.scoring_handler:
            status['scoring_handler'] = self.scoring_handler.get_scoring_stats()
        
        
        if self.voice_handler:
            status['voice_handler'] = self.voice_handler.get_handler_status()
        
        
        if self.screenshare_handler:
            status['screenshare_handler'] = {'initialized': True}
        
        
        status['error_handler'] = self.error_handler.get_error_statistics()
        
        return status
    
    async def _handle_ping(self, websocket: WebSocketServerProtocol, message: dict) -> None:
        try:
            pong_message = {
                "type": "pong",
                "timestamp": asyncio.get_event_loop().time()
            }
            
            
            if "request_id" in message:
                pong_message["request_id"] = message["request_id"]
            
            await self.send_to_client(websocket, pong_message)
            self.logger.debug("Sent pong response to client")
            
        except Exception as e:
            self.error_handler.log_error("PING_HANDLER", f"Error handling ping: {e}", e)
    
    async def _handle_pong(self, websocket: WebSocketServerProtocol, message: dict) -> None:
        try:
            self.logger.debug("Received pong from client")
            
            
        except Exception as e:
            self.error_handler.log_error("PONG_HANDLER", f"Error handling pong: {e}", e)
    
    async def _handle_callback_response(self, message: dict) -> None:
        try:
            request_id = message.get('request_id')
            if not request_id:
                self.logger.warning("Callback response missing request_id")
                return
            
            
            if message.get('error'):
                error_msg = message.get('error', 'Unknown error')
                error = Exception(f"Request failed: {error_msg}")
                self.callback_manager.reject_request(request_id, error)
                self.logger.debug(f"Rejected callback request {request_id} with error: {error_msg}")
            else:
                
                response_data = {k: v for k, v in message.items() if k not in ['request_id', 'type']}
                self.callback_manager.resolve_request(request_id, response_data)
                self.logger.debug(f"Resolved callback request {request_id}")
                
        except Exception as e:
            self.error_handler.log_error("CALLBACK_RESPONSE", f"Error handling callback response: {e}", e)
    
    async def _monitor_connection_health(self) -> None:
        try:
            while True:
                await asyncio.sleep(60)  
                
                if not self.enabled:
                    break
                
                
                health_report = await self.error_handler.monitor_connection_health()
                
                
                stale_clients = set()
                for client in self.clients.copy():
                    try:
                        
                        if client.closed:
                            stale_clients.add(client)
                    except Exception as e:
                        self.logger.debug(f"Error checking client connection health: {e}")
                        stale_clients.add(client)
                
                
                for client in stale_clients:
                    self.clients.discard(client)
                    self.logger.debug("Removed stale client connection")
                
                if stale_clients:
                    self.logger.info(f"Cleaned up {len(stale_clients)} stale connections")
                
                
                if health_report['connection_success_rate'] < 80.0 and health_report['total_connections'] > 5:
                    self.logger.warning(
                        f"Low connection success rate detected: {health_report['connection_success_rate']:.1f}% "
                        f"({health_report['failed_connections']}/{health_report['total_connections']} failed)"
                    )
                
                total_errors = sum(health_report['error_counts'].values())
                if total_errors > 50:  
                    self.logger.warning(f"High error count detected: {total_errors} total errors")
                
        except asyncio.CancelledError:
            self.logger.debug("Connection health monitoring cancelled")
        except Exception as e:
            await self.error_handler.handle_critical_error("Connection Health Monitor", e)
    
    def register_handler(self, message_type: str, handler_func) -> None:
        self.handlers[message_type] = handler_func
        self.logger.debug(f"Registered handler for message type: {message_type}")
    
    def unregister_handler(self, message_type: str) -> None:
        if message_type in self.handlers:
            del self.handlers[message_type]
            self.logger.debug(f"Unregistered handler for message type: {message_type}")
    
    async def send_request_with_response(
        self,
        message_type: str,
        message_data: Dict[str, Any],
        timeout: Optional[float] = None
    ) -> Any:
        if not self.enabled:
            raise Exception("WebSocket system is disabled")
        
        async def send_func(request_id: str, message: Dict[str, Any]) -> None:
            await self.broadcast(message)
        
        return await self.request_response_handler.send_request_with_response(
            send_func=send_func,
            message_type=message_type,
            message_data=message_data,
            timeout=timeout
        )
    
    async def send_request_no_response(
        self,
        message_type: str,
        message_data: Dict[str, Any]
    ) -> str:
        if not self.enabled:
            raise Exception("WebSocket system is disabled")
        
        async def send_func(request_id: str, message: Dict[str, Any]) -> None:
            await self.broadcast(message)
        
        return await self.request_response_handler.send_request_no_response(
            send_func=send_func,
            message_type=message_type,
            message_data=message_data
        )
    
    def get_callback_stats(self) -> Dict[str, Any]:
        return {
            'pending_requests': self.callback_manager.get_pending_count(),
            'default_timeout': self.callback_manager.default_timeout
        }
    
    def _serialize_message(self, message: dict) -> str:
        try:
            return json.dumps(message)
        except Exception as e:
            self.logger.error(f"Error serializing message: {e}")
            raise
    
    def _initialize_handlers(self) -> None:
        try:
            
            self.player_handler = PlayerHandler(self)
            self.logger.info("PlayerHandler initialized successfully")
            
            
            from .websocket.handlers.queue_handler import QueueHandler
            self.queue_handler = QueueHandler(self)
            self.logger.info("QueueHandler initialized successfully")
            
            
            from .websocket.handlers.game_handler import GameHandler
            self.game_handler = GameHandler(self)
            self.logger.info("GameHandler initialized successfully")
            
            
            from .websocket.handlers.scoring_handler import ScoringHandler
            self.scoring_handler = ScoringHandler(self)
            self.logger.info("ScoringHandler initialized successfully")
            
            
            from .websocket.handlers.voice_handler import VoiceHandler
            self.voice_handler = VoiceHandler(self)
            self.logger.info("VoiceHandler initialized successfully")
            
            
            from .websocket.handlers.screenshare_handler import ScreenshareHandler
            self.screenshare_handler = ScreenshareHandler(self)
            
            
            self.register_handler("autoss", self.screenshare_handler.handle_auto_ss)
            self.register_handler("screensharedontlog", self.screenshare_handler.handle_screenshare_dontlog)
            
            self.logger.info("ScreenshareHandler initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Error initializing message handlers: {e}")
            raise e
    
    def get_error_statistics(self) -> Dict[str, Any]:
        return self.error_handler.get_error_statistics()
    
    def reset_error_statistics(self) -> None:
        self.error_handler.reset_error_statistics()
        self.logger.info("WebSocket error statistics have been reset")
    
    def register_error_recovery_callback(self, error_type: str, callback: Callable) -> None:
        self.error_handler.register_recovery_callback(error_type, callback)
    
    def unregister_error_recovery_callback(self, error_type: str, callback: Callable) -> None:
        self.error_handler.unregister_recovery_callback(error_type, callback)
    
    async def cleanup_resources(self) -> None:
        try:
            
            self.warp_timeouts.clear()
            self.warp_attempts.clear()
            self.team_data.clear()
            
            
            handlers_to_cleanup = [
                ("PlayerHandler", self.player_handler),
                ("QueueHandler", self.queue_handler),
                ("GameHandler", self.game_handler),
                ("ScoringHandler", self.scoring_handler),
                ("VoiceHandler", self.voice_handler),
                ("ScreenshareHandler", self.screenshare_handler)
            ]
            
            for handler_name, handler in handlers_to_cleanup:
                if handler:
                    try:
                        if hasattr(handler, 'cleanup') and callable(handler.cleanup):
                            await handler.cleanup()
                        setattr(self, handler_name.lower().replace('handler', '_handler'), None)
                    except Exception as e:
                        await self.error_handler.handle_shutdown_error(f"{handler_name} Cleanup", e)
            
            self.logger.debug("WebSocket resources cleaned up")
            
        except Exception as e:
            await self.error_handler.handle_shutdown_error("Resource Cleanup", e)
