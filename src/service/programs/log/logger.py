class Logger:

    @staticmethod
    def err(message: str) -> None:
        print(f"[ERR]", message)

    @staticmethod
    def frontend(message: str) -> None:
        print(f"[FRONT]", message)

    @staticmethod
    def backend(message: str) -> None:
        print(f"[BACK]", message)

    @staticmethod
    def topic_message(topic: str, msg_id: int, content: str) -> None:
        print(f"[RECEIVED] {topic}-{msg_id} :: {content}")

    @staticmethod
    def put_message(topic: str, msg_id: int, content: str) -> None:
        print(f"[SENT] {topic}-{msg_id} :: {content}")

    @staticmethod
    def ack(identity: int, topic: str, msg_id: int) -> None:
        print(f"[ACK] {topic}-{msg_id} :: {identity} ")

    @staticmethod
    def get(identity: int, topic: str) -> None:
        print(f"[GET] {topic} :: {identity} ")

    @staticmethod
    def subscribe(topic: str) -> None:
        print(f"[SUBSCRIBE] {topic}")
