Mox.defmock(AwsClientMock, for: ExAws.Behaviour)
Application.put_env(:remote_persistent_term, :aws_client, AwsClientMock)

ExUnit.start()
