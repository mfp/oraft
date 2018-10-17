val make_client_wrapper :
  client_config:Tls.Config.client ->
  [`Outgoing] Oraft_lwt.conn_wrapper

val make_server_wrapper :
  client_config:Tls.Config.client ->
  server_config:Tls.Config.server ->
  [`Incoming | `Outgoing] Oraft_lwt.conn_wrapper
