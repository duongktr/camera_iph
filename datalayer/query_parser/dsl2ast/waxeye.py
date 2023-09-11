# Waxeye Parser Generator
# www.waxeye.org
# Copyright (C) 2008-2010 Orlando Hill
# Licensed under the MIT license. See 'LICENSE' for details.
import json
from typing import List


class Edge:
    def __init__(self, trans, state, voided):
        self.trans = trans
        self.state = state
        self.voided = voided

    def __str__(self):
        return f"Edge{{ trans: {self.trans}, state: {self.state}, voided: {self.voided} }}"


class State:
    def __init__(self, edges, match):
        self.edges = edges
        self.match = match

    def __str__(self):
        string = "State{ edges: ["
        for edge in self.edges:
            string = string + str(edge) + ", "
        string += "], match = {match} }"
        return string.format(match=self.match)


class FA:
    VOID = 0
    PRUNE = 1
    LEFT = 2
    POS = 3
    NEG = 4

    def __init__(self, type, states, mode):
        self.type = type
        self.states = states
        self.mode = mode


class ParseError:
    def __init__(self, pos, line, col, nt):
        self.pos = pos
        self.line = line
        self.col = col
        self.nt = nt

    def __str__(self):
        return (
                f"parse error: failed to match '{self.nt}' at line={self.line}, col={self.col}, pos={self.pos}"
        )

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, indent=None)

    def to_dict(self):
        return json.loads(self.to_json())


class AST:
    def __init__(self, type, children, pos):
        self.type = type
        self.children = children
        self.pos = pos

    def str_iter(self, ast, indent, acc):
        for i in range(0, indent[0] - 1):
            acc.append("    ")
        if indent[0] > 0:
            acc.append("->  ")
        acc.append(ast.type)
        indent[0] += 1
        for a in ast.children:
            acc.append("\n")
            if isinstance(a, AST):
                self.str_iter(a, indent, acc)
            else:
                for i in range(0, indent[0] - 1):
                    acc.append("    ")
                if indent[0] > 0:
                    acc.append("|   ")
                acc.append(a)
        indent[0] -= 1

    def __str__(self):
        acc = []
        self.str_iter(self, [0], acc)
        return "".join(acc)

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, indent=None)

    def to_dict(self):
        return json.loads(self.to_json())


class WaxeyeParser:
    def __init__(self, start, eof_check, automata):
        self.start = start
        self.eof_check = eof_check
        self.automata = automata

    def parse(self, input):
        return WaxeyeParser.InnerParser(
            self.start, self.eof_check, self.automata, input
        ).parse()

    class InnerParser:
        def __init__(self, start, eof_check, automata: List[FA], input):
            self.start = start
            self.eof_check = eof_check
            self.automata = automata
            self.input = input
            self.input_len = len(input)
            self.input_pos = 0
            self.line = 1
            self.column = 0
            self.last_cr = False
            self.error_pos = 0
            self.error_line = 1
            self.error_col = 0
            self.error_nt = automata[start].type
            self.fa_stack = []
            self.cache = {}
            self.func_stack = []
            self.result = False

        def parse(self):
            return self.do_eof_check(self.match_automaton_with_stack())

        def match_automaton_with_stack(self):
            index = self.start
            start_pos = self.input_pos
            start_line = self.line
            start_col = self.column
            start_cr = self.last_cr
            automaton = self.automata[index]

            # Examine if that case has been cached
            key = (index, start_pos)
            if key in self.cache:
                cachedItem = self.cache[key]
                self.restore_pos(
                    cachedItem[1], cachedItem[2], cachedItem[3], cachedItem[4]
                )
                return cachedItem[0]

            self.fa_stack = [automaton] + self.fa_stack
            automata = (
                "automaton",
                True,
                index,
                start_pos,
                start_line,
                start_col,
                start_cr,
            )
            new_func = ("state", False, 0)
            self.func_stack = [new_func, automata] + self.func_stack

            # Begin while
            while self.func_stack:
                # If top of stack is match_automaton
                func_item = self.func_stack[0]
                if func_item[0] == "automaton":
                    (
                        _,
                        _,
                        index,
                        start_pos,
                        start_line,
                        start_col,
                        start_cr,
                    ) = func_item
                    automaton = self.automata[index]
                    type_ = automaton.type
                    mode = automaton.mode
                    key = (index, start_pos)

                    self.fa_stack = self.fa_stack[1:]
                    res = self.result

                    if mode == FA.POS:
                        self.restore_pos(
                            start_pos, start_line, start_col, start_cr
                        )
                        if res != False:
                            value = True
                        else:
                            value = False
                    elif mode == FA.NEG:
                        self.restore_pos(
                            start_pos, start_line, start_col, start_cr
                        )
                        if res != False:
                            value = self.update_error()
                        else:
                            value = True
                    elif res != False:
                        if mode == FA.VOID:
                            value = True
                        elif mode == FA.PRUNE:
                            l = len(res)
                            if l == 0:
                                value = True
                            elif l == 1:
                                value = res[0]
                            else:
                                value = AST(
                                    type_, res, (start_pos, self.input_pos)
                                )
                        else:
                            value = AST(type_, res, (start_pos, self.input_pos))
                    else:
                        value = self.update_error()

                    self.cache[key] = (
                        value,
                        self.input_pos,
                        self.line,
                        self.column,
                        self.last_cr,
                    )
                    self.result = value
                    self.func_stack = self.func_stack[1:]
                    if not self.func_stack:
                        break
                    else:  # Take some match_state's jobs after match_automaton return til match_state calls itself
                        if value:
                            current_func = self.func_stack[0]
                            _index = current_func[2]
                            state = self.fa_stack[0].states[_index]
                            edges = state.edges
                            current_edge_index = current_func[7]
                            new_func = (
                                "state",
                                False,
                                edges[current_edge_index].state,
                            )
                            # Add new element to make stack form:
                            # ("state", True, _index, start_pos, start_line, start_col, start_cr, current_edge_index, value)
                            self.func_stack[0] = current_func + (value,)
                            self.func_stack = [new_func] + self.func_stack

                # If top of stack is match_state and being visited for first time
                func_item = self.func_stack[0]
                if func_item[0] == "state" and func_item[1] is False:
                    self.match_state(-1, func_item)

                # Otherwise: handle match_state case inside match_edge
                func_item = self.func_stack[0]
                if func_item[0] == "state" and func_item[1] is True:
                    index = func_item[2]
                    state = self.fa_stack[0].states[index]
                    edges = state.edges
                    start_pos = func_item[3]
                    start_line = func_item[4]
                    start_col = func_item[5]
                    start_cr = func_item[6]
                    current_edge_index = func_item[7]

                    # match_state
                    if len(func_item) == 9:
                        res = func_item[8]
                        tran_res = self.result
                        if tran_res != False:
                            if edges[current_edge_index].voided or res is True:
                                self.result = tran_res
                            else:
                                self.result = [res] + tran_res
                            self.func_stack = self.func_stack[1:]
                            continue
                        else:
                            self.restore_pos(
                                start_pos, start_line, start_col, start_cr
                            )

                    self.match_state(current_edge_index, func_item)
            # End while

            return self.result

        def match_state(self, _current_edge_index, current_func):
            index = current_func[2]
            state = self.fa_stack[0].states[index]
            edges = state.edges
            is_break = False
            for current_edge_index, edge in enumerate(edges):
                if current_edge_index <= _current_edge_index:
                    continue
                #  match edge
                start_pos = self.input_pos
                start_line = self.line
                start_col = self.column
                start_cr = self.last_cr
                t = edge.trans

                if isinstance(t, str):
                    if (
                            self.input_pos < self.input_len
                            and t == self.input[self.input_pos]
                    ):
                        res = self.mv()
                    else:
                        res = self.update_error()
                elif t == -1:  # use -1 for wild card
                    if self.input_pos < self.input_len:
                        res = self.mv()
                    else:
                        res = self.update_error()
                elif isinstance(t, int):
                    key = (t, start_pos)

                    if self.cache.__contains__(key):
                        cached_item = self.cache[key]
                        self.restore_pos(
                            cached_item[1],
                            cached_item[2],
                            cached_item[3],
                            cached_item[4],
                        )
                        res = cached_item[0]
                    else:
                        # Do the first jobs of match_automaton then push match_automaton and match_state into Stack
                        _index = t
                        automaton = self.automata[_index]

                        self.fa_stack = [automaton] + self.fa_stack
                        self.func_stack[0] = (
                            "state",
                            True,
                            index,
                            start_pos,
                            start_line,
                            start_col,
                            start_cr,
                            current_edge_index,
                        )
                        automata = (
                            "automaton",
                            True,
                            _index,
                            start_pos,
                            start_line,
                            start_col,
                            start_cr,
                        )
                        new_func = ("state", False, 0)
                        self.func_stack = [
                                              new_func,
                                              automata,
                                          ] + self.func_stack
                        is_break = True
                        break
                elif isinstance(t, list):
                    if self.input_pos < self.input_len and self.within_set(t, ord(self.input[self.input_pos])):
                        res = self.mv()
                    else:
                        res = self.update_error()
                else:
                    res = False

                if res:
                    # Push match_state into Stack
                    new_func = ("state", False, edge.state)
                    self.func_stack[0] = (
                        "state",
                        True,
                        index,
                        start_pos,
                        start_line,
                        start_col,
                        start_cr,
                        current_edge_index,
                        res,
                    )
                    self.func_stack = [new_func] + self.func_stack
                    is_break = True
                    break
            # If all elements are not match
            if not is_break:
                self.result = state.match and []
                self.func_stack = self.func_stack[1:]

        def restore_pos(self, pos, line, col, cr):
            self.input_pos = pos
            self.line = line
            self.column = col
            self.last_cr = cr

        def update_error(self):
            if self.error_pos < self.input_pos:
                self.error_pos = self.input_pos
                self.error_line = self.line
                self.error_col = self.column
                self.error_nt = self.fa_stack[0].type
            return False

        def mv(self):
            ch = self.input[self.input_pos]
            self.input_pos += 1

            if ch == "\r":
                self.line += 1
                self.column = 0
                self.last_cr = True
            else:
                if ch == "\n":
                    if not self.last_cr:
                        self.line += 1
                        self.column = 0
                else:
                    self.column += 1
                self.last_cr = False

            return ch

        def do_eof_check(self, res):
            if res:
                if self.eof_check and self.input_pos < self.input_len:
                    # Create a parse error - Not all input consumed
                    return ParseError(
                        self.error_pos,
                        self.error_line,
                        self.error_col,
                        self.error_nt,
                    )
                else:
                    return res
            else:
                # Create a parse error
                return ParseError(
                    self.error_pos,
                    self.error_line,
                    self.error_col,
                    self.error_nt,
                )

        # Takes a set and an ordinal
        def within_set(self, set, c):
            if not set:
                return False
            else:
                aa = set[0]
                if isinstance(aa, str):
                    if ord(aa) == c:
                        return True
                    else:
                        if ord(aa) < c:
                            return self.within_set(set[1:], c)
                        else:
                            return False
                else:
                    # If not a String then must be a range (tuple)
                    if aa[0] <= c <= aa[1]:
                        return True
                    else:
                        if aa[1] < c:
                            return self.within_set(set[1:], c)
                        else:
                            return False
