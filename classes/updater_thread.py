import time
from threading import Thread


class updater_thread(Thread):
    def __init__(
        self, func, update_interval, num_registers_to_check, db_route
    ):
        """
        Initializer for a thread in daemon mode.

        @param func - the function to run
        @param update_interval - interval where the function is executed
        @param num_registers_to_check - numbers of registers to check in db
        @param db_route - database to work on
        """
        Thread.__init__(self)
        self.daemon = True
        self.func = func
        self.update_interval = update_interval
        self.num_registers_to_check = num_registers_to_check
        self.db_route = db_route
        self.start()

    def run(self):
        """
        Runs when start() is called.
        Update variables every time it loops.
        """
        while True:
            self.func(
                self.num_registers_to_check,
                self.db_route,
            )
            time.sleep(self.update_interval)
