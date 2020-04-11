from collections import Counter


class Menu:
    def __init__(self, options, launcher_ref):
        self.alias_to_method = {}
        self.launcher_ref = launcher_ref

        alias_counter = Counter(alias for option in options for alias in option.aliases)
        most_common_alias, most_common_count = alias_counter.most_common(1)[0]
        if most_common_count > 1:
            raise ValueError(f"Alias {most_common_alias} not unique")

        for option in options:
            self.alias_to_method.update(dict.fromkeys(option.aliases, option.method_ref))

    def wait_for_input(self):
        while True:
            try:
                print("Waiting for input")
                self.parse_command(input())
                print()
            except KeyboardInterrupt:
                continue

    def parse_command(self, line):
        menu_option_name, space, arguments = line.strip().partition(" ")
        if menu_option_name in self.alias_to_method.keys():
            return self.launcher_ref(self.alias_to_method[menu_option_name], arguments.split())
        else:
            print("Alias not known")
            return True

    def __len__(self):
        return self.alias_to_method.__len__()


class MenuOption:
    def __init__(self, aliases, method_ref):
        self.aliases = aliases
        self.method_ref = method_ref

    def __repr__(self):
        return f"{', '.join(self.aliases)}: {self.method_ref}"
