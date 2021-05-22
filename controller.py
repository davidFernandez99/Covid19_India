import resources as res
import logic


def run(client):
    """
    The user will be asked to enter a command and depending on it, one procedure or another will be completed.
    :param client:
    :return:
    """

    exit_program = False
    while not exit_program:

        command = input("\n>>").strip()

        if command in ("close", "quit", "exit"):
            exit_program = True

        #elif "select database" in command.lower():
        #    logic.select_database(command, client)

        elif "insert" in command.lower():
            logic.insert_point(command, client)

        elif "update" in command.lower():
            logic.update_point(command, client)

        elif "delete" in command.lower():
            logic.delete_point(command, client)

        elif command.lower() in ("h", "help", "-h", "-help", "--h", "--help"):
            res.show_help_commands()

        else:
            res.show_error_command(command)