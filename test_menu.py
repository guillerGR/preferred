from unittest import TestCase

from menu import Menu, MenuOption


class TestMenu(TestCase):
    def test_menu_without_duplicate_aliases(self):
        menu = Menu([MenuOption(["alias", "alias3"], "method"), MenuOption(["alias2"], "method2")])
        self.assertEqual(3, menu.__len__())

    def test_menu_with_duplicate_aliases_from_different_options(self):
        with self.assertRaises(ValueError):
            Menu([MenuOption(["alias", "alias3"], "method"), MenuOption(["alias"], "method2")])

    def test_menu_with_duplicate_aliases_from_same_option(self):
        with self.assertRaises(ValueError):
            Menu([MenuOption(["alias", "alias"], "method"), MenuOption(["alias2"], "method2")])

    def test_menu_with_duplicate_aliases_from_both_same_and_different_options(self):
        with self.assertRaises(ValueError):
            Menu([MenuOption(["alias", "alias"], "method"), MenuOption(["alias"], "method2")])
