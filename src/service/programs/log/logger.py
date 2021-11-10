class Logger:

    def err(message: str) -> None: 
        print (f"[  ERR  ]", message)

    def frontend(message: str) -> None:
        print(f"[ FRONT ]", message)

    def backend(message: str) -> None:
        print(f"[ BACK  ]", message)