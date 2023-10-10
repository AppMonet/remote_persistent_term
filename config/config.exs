import Config

config :remote_persistent_term,
  aws_client: ExAws

import_config "#{Mix.env()}.exs"
