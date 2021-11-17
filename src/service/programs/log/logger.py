import sys

class Logger:

    # --------------------------------------------------------------------------
    # Server logs
    # --------------------------------------------------------------------------

    @staticmethod
    def new_message(message: list) -> None:
        # return
        print("\n" + "-" * 80)
        print(f"{message}")
        print("-" * 80)

    @staticmethod
    def subscription(client_id: int, topic: str) -> None:
        print(f"SUB {client_id} - '{topic}'")

    @staticmethod
    def unsubscription(client_id: int, topic: str) -> None:
        print(f"UNSUB {client_id} - '{topic}'")

    @staticmethod
    def publication(topic: str, message_id: int, message: str):
        print(f"PUT '{topic}' - {message_id} - '{message}'")

    @staticmethod
    def request(client_id: int, topic: str):
        print(f"GET {client_id} - '{topic}'")

    @staticmethod
    def acknowledgement(client_id: int, topic: str, message_id: int):
        print(f"ACK {client_id} - '{topic}' - {message_id}")

    @staticmethod
    def info(message="", end="\n"):
        print(message, end=end)

    @staticmethod
    def success(message="", end="\n"):
        sys.stdout.write('\033[1;32m')
        print(message, end=end)
        sys.stdout.write('\033[0;0m')

    @staticmethod
    def warning(message):
        sys.stdout.write('\033[1;33m')
        print(message)
        sys.stdout.write('\033[0;0m')

    # --------------------------------------------------------------------------
    # Subscriber logs
    # --------------------------------------------------------------------------

    @staticmethod
    def topic_message(topic: str, msg_id: int, content: str) -> None:
        print(f"[RECEIVED] {topic}-{msg_id} :: {content}")

    @staticmethod
    def get(identity: int, topic: str) -> None:
        print(f"[GET] {topic} :: {identity} ")

    @staticmethod
    def subscribe(topic: str) -> None:
        print(f"[SUBSCRIBE] {topic}")

    @staticmethod
    def unsubscribe(topic: str) -> None:
        print(f"[UNSUBSCRIBE] {topic}")

    # --------------------------------------------------------------------------
    # Publisher logs
    # --------------------------------------------------------------------------

    @staticmethod
    def put_message(topic: str, msg_id: int, content: str) -> None:
        print(f"[SENT] {topic}-{msg_id} :: {content}")