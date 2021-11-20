class AWSContext:
    def __init__(self) -> None:
        self.get_remaining_time_in_millis = lambda: float()
        self.function_name = str()
        self.function_version = str()
        self.invoked_function_arn = str()
        self.memory_limit_in_mb = str()
        self.aws_request_id = str()
        self.log_group_name = str()
        self.log_stream_name = str()
