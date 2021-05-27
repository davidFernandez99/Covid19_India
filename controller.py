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

        command = input("\n>>").strip().lower()

        if command in ("close", "quit", "exit"):
            exit_program = True

        elif "insert" in command:
            logic.insert_point(command, client)

        elif "update" in command:
            logic.update_point(command, client)

        elif "delete" in command:
            logic.delete_point(command, client)

        elif command in ("h", "help", "-h", "-help", "--h", "--help"):
            res.show_help_commands()

        else:
            res.show_error_command(command)
