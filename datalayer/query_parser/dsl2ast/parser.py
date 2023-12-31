# Generated by the Waxeye Parser Generator - version 0.8.0
# www.waxeye.org
from .waxeye import Edge, State, FA, WaxeyeParser


class Parser (WaxeyeParser):
    start = 0
    eof_check = True
    automata = [FA("dsl", [State([Edge(20, 1, False)], False),
            State([Edge(2, 2, False)], False),
            State([Edge(3, 3, False)], True),
            State([Edge(2, 2, False)], False)], FA.LEFT),
        FA("compare", [State([Edge(6, 1, False),
                Edge(5, 1, False),
                Edge(4, 1, False)], False),
            State([], True)], FA.LEFT),
        FA("unary", [State([Edge(20, 1, False),
                Edge(1, 7, False)], False),
            State([Edge("(", 2, True)], False),
            State([Edge(20, 3, False)], False),
            State([Edge(0, 4, False)], False),
            State([Edge(20, 5, False)], False),
            State([Edge(")", 6, True)], False),
            State([Edge(20, 7, False)], False),
            State([], True)], FA.PRUNE),
        FA("logic", [State([Edge("A", 1, False),
                Edge("O", 4, False)], False),
            State([Edge("N", 2, False)], False),
            State([Edge("D", 3, False)], False),
            State([], True),
            State([Edge("R", 3, False)], False)], FA.LEFT),
        FA("equals", [State([Edge(20, 1, False)], False),
            State([Edge(7, 2, False)], False),
            State([Edge(20, 3, False)], False),
            State([Edge("!", 4, False),
                Edge("~", 5, False),
                Edge("!", 9, False),
                Edge("=", 5, False)], False),
            State([Edge("~", 5, False)], False),
            State([Edge(20, 6, False)], False),
            State([Edge(9, 7, False)], False),
            State([Edge(20, 8, False)], False),
            State([], True),
            State([Edge("=", 5, False)], False)], FA.LEFT),
        FA("inequality", [State([Edge(20, 1, False)], False),
            State([Edge(7, 2, False)], False),
            State([Edge(20, 3, False)], False),
            State([Edge(">", 4, False),
                Edge("<", 9, False),
                Edge(["<", ">"], 5, False)], False),
            State([Edge("=", 5, False)], False),
            State([Edge(20, 6, False)], False),
            State([Edge(17, 7, False),
                Edge(15, 7, False),
                Edge(16, 7, False)], False),
            State([Edge(20, 8, False)], False),
            State([], True),
            State([Edge("=", 5, False)], False)], FA.LEFT),
        FA("allField", [State([Edge(20, 1, False)], False),
            State([Edge(17, 2, False),
                Edge(16, 2, False)], False),
            State([Edge(20, 3, False)], False),
            State([], True)], FA.LEFT),
        FA("field", [State([Edge([(65, 90), "_", (97, 122)], 1, False)], False),
            State([Edge([(45, 46), (48, 57), (65, 90), "_", (97, 122)], 1, False)], True)], FA.LEFT),
        FA("operator", [State([Edge("!", 1, False),
                Edge("<", 3, False),
                Edge(">", 4, False),
                Edge("!", 5, False),
                Edge([(60, 62), "~"], 2, False)], False),
            State([Edge("=", 2, False)], False),
            State([], True),
            State([Edge("=", 2, False)], False),
            State([Edge("=", 2, False)], False),
            State([Edge("~", 2, False)], False)], FA.LEFT),
        FA("value", [State([Edge(17, 1, False),
                Edge(15, 1, False),
                Edge(16, 1, False),
                Edge(19, 1, False),
                Edge(10, 1, False)], False),
            State([Edge(20, 2, False)], False),
            State([], True)], FA.LEFT),
        FA("function", [State([Edge(11, 1, False),
                Edge(12, 1, False),
                Edge(13, 1, False)], False),
            State([], True)], FA.LEFT),
        FA("term", [State([Edge("T", 1, True)], False),
            State([Edge("E", 2, True)], False),
            State([Edge("R", 3, True)], False),
            State([Edge("M", 4, True)], False),
            State([Edge("(", 5, True)], False),
            State([Edge(20, 6, False)], False),
            State([Edge(16, 7, False)], False),
            State([Edge(20, 8, False)], False),
            State([Edge(")", 9, True)], False),
            State([], True)], FA.LEFT),
        FA("in", [State([Edge("I", 1, True)], False),
            State([Edge("N", 2, True)], False),
            State([Edge("(", 3, True)], False),
            State([Edge(20, 4, False)], False),
            State([Edge(14, 5, False)], False),
            State([Edge(20, 6, False)], False),
            State([Edge(")", 7, True)], False),
            State([], True)], FA.LEFT),
        FA("notin", [State([Edge("N", 1, True)], False),
            State([Edge("O", 2, True)], False),
            State([Edge("T", 3, True)], False),
            State([Edge("I", 4, True)], False),
            State([Edge("N", 5, True)], False),
            State([Edge("(", 6, True)], False),
            State([Edge(20, 7, False)], False),
            State([Edge(14, 8, False)], False),
            State([Edge(20, 9, False)], False),
            State([Edge(")", 10, True)], False),
            State([], True)], FA.LEFT),
        FA("array", [State([Edge("[", 1, True)], False),
            State([Edge(20, 2, False)], False),
            State([Edge(9, 3, False),
                Edge("]", 5, True)], False),
            State([Edge(21, 4, False),
                Edge("]", 5, True)], False),
            State([Edge(9, 3, False)], False),
            State([], True)], FA.LEFT),
        FA("number", [State([Edge("-", 1, False),
                Edge([(48, 57)], 2, False)], False),
            State([Edge([(48, 57)], 2, False)], False),
            State([Edge([(48, 57)], 2, False),
                Edge(".", 3, False)], True),
            State([Edge([(48, 57)], 4, False)], False),
            State([Edge([(48, 57)], 4, False)], True)], FA.LEFT),
        FA("string", [State([Edge("\"", 1, True)], False),
            State([Edge("\\", 2, True),
                Edge(23, 3, False),
                Edge("\"", 5, True)], False),
            State([Edge(18, 1, False)], False),
            State([Edge(22, 4, False)], False),
            State([Edge(-1, 1, False)], False),
            State([], True)], FA.LEFT),
        FA("ipValue", [State([Edge([(48, 57)], 1, False)], False),
            State([Edge([(48, 57)], 1, False),
                Edge(".", 2, False)], False),
            State([Edge([(48, 57)], 3, False)], False),
            State([Edge([(48, 57)], 3, False),
                Edge(".", 4, False)], False),
            State([Edge([(48, 57)], 5, False)], False),
            State([Edge([(48, 57)], 5, False),
                Edge(".", 6, False)], False),
            State([Edge([(48, 57)], 7, False)], False),
            State([Edge([(48, 57)], 7, False),
                Edge("/", 8, False)], True),
            State([Edge([(48, 57)], 9, False)], False),
            State([Edge([(48, 57)], 9, False)], True)], FA.LEFT),
        FA("escaped", [State([Edge("u", 1, False),
                Edge(["\"", "\'", "/", "\\", "b", "f", "n", "r", "t"], 5, False)], False),
            State([Edge([(48, 57), (65, 70), (97, 102)], 2, False)], False),
            State([Edge([(48, 57), (65, 70), (97, 102)], 3, False)], False),
            State([Edge([(48, 57), (65, 70), (97, 102)], 4, False)], False),
            State([Edge([(48, 57), (65, 70), (97, 102)], 5, False)], False),
            State([], True)], FA.LEFT),
        FA("literal", [State([Edge("T", 1, False),
                Edge("F", 5, False),
                Edge("N", 9, False)], False),
            State([Edge("R", 2, False)], False),
            State([Edge("U", 3, False)], False),
            State([Edge("E", 4, False)], False),
            State([], True),
            State([Edge("A", 6, False)], False),
            State([Edge("L", 7, False)], False),
            State([Edge("S", 8, False)], False),
            State([Edge("E", 4, False)], False),
            State([Edge("U", 10, False)], False),
            State([Edge("L", 11, False)], False),
            State([Edge("L", 4, False)], False)], FA.LEFT),
        FA("ws", [State([Edge([(9, 10), "\r", " "], 0, False)], True)], FA.VOID),
        FA("com", [State([Edge(",", 1, False)], False),
            State([Edge(20, 2, False)], False),
            State([], True)], FA.VOID),
        FA("", [State([Edge("\"", 1, False)], False),
            State([], True)], FA.NEG),
        FA("", [State([Edge("\\", 1, False)], False),
            State([], True)], FA.NEG)]

    def __init__(self):
        WaxeyeParser.__init__(self, Parser.start, Parser.eof_check, Parser.automata)
