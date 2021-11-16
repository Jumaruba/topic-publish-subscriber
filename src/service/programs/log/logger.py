class Logger:

    @staticmethod
    def subscription(client_id: int, topic: str) -> None:
        return
        print(f"[SUBSCRIPTION] {topic} :: {client_id}")

    @staticmethod
    def err(message: str) -> None:
        return
        print(f"[ERR]", message)

    @staticmethod
    def frontend(message: str) -> None:
        return
        print(f"[FRONT]", message)

    @staticmethod
    def backend(message: str) -> None:
        return
        print(f"[BACK]", message)

    @staticmethod
    def topic_message(topic: str, msg_id: int, content: str) -> None:
        return
        print(f"[RECEIVED] {topic}-{msg_id} :: {content}")

    @staticmethod
    def put_message(topic: str, msg_id: int, content: str) -> None:
        return
        print(f"[SENT] {topic}-{msg_id} :: {content}")

    @staticmethod
    def ack(identity: int, topic: str, msg_id: int) -> None:
        return
        print(f"[ACK] {topic}-{msg_id} :: {identity} ")

    @staticmethod
    def get(identity: int, topic: str) -> None:
        return
        print(f"[GET] {topic} :: {identity} ")

    @staticmethod
    def subscribe(topic: str) -> None:
        return
        print(f"[SUBSCRIBE] {topic}")
