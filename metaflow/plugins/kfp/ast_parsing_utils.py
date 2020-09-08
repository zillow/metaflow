import ast

"""
This file will provide useful utilities to parse ASTs of Metaflow functions.
For now, it will focus on extracting the length of the iterable in a foreach 
branch (aka 'num_splits'), as this value is necesary to properly orchestrate
a foreach run.
"""

def return_iterable_length_from_identifier(func_ast, identifier):
    """
    This function takes the identifier of a iterable (list, list created by comprehension,
    dictionary, set, etc), and return the length of the iterable.
    NOTE: currently only supports lists created by comprehension.
    """
    for statement in func_ast.body:
        if isinstance(statement, ast.Assign):
            for i, target in enumerate(statement.targets):
                if target.attr == identifier:
                    return statement.value.generators[i].iter.args[0].n # TODO: add support for lists and dictionaries here
    
    raise ValueError("Error: no valid iterable defined. Please use a list by comprehension \
                        and specify the iterable in one line with no tuples. \
                        e.g. self.list = [x for x in range(10)]")

def return_iterable_identifier(func_ast):
    """
    This function will return the identifier name of the iterable that causes a foreach branch.
    e.g. in `self.next(self.explore, foreach='list_to_explore')`, this function returns "list_to_explore".
    This will be used in another function (TODO: specify here) to extract the length of the iterable.
    """
    # iterable_name = None
    # starting from bottom of function because "self.next" will be closer (if not at) end of the function
    for statement in reversed(func_ast.body): 
        if isinstance(statement, ast.Expr):
            if statement.value.func.attr == "next": # we need to see what self.next specifies
                for keyword in statement.value.keywords:
                    if keyword.arg == "foreach": # we see the foreach specification
                        iterable_name = keyword.value.s # return identifier, e.g. 'list_to_explore'
                        return iterable_name
    
    raise ValueError("Error: either `self.next() not specified for foreach not specified is a `self.next()`")

def return_iterable_length(func_ast):
    """
    Some personal notes on how to parse the AST.

    # graph.nodes['start'].func_ast.body[4].value.func.attr 
    # graph.nodes['start'].func_ast.body[4].value.keywords[0].arg
    # graph.nodes['start'].func_ast.body[4].value.keywords[0].value.s

    # graph.nodes['start'].func_ast.body[2].targets[0].attr # gets the iterable name 'list_to_explore'
    # graph.nodes['start'].func_ast.body[2].value.generators[0].iter.args[0].n # gets the integer n=10!
    """
    iterable_name = return_iterable_identifier(func_ast)
    iterable_length = return_iterable_length_from_identifier(func_ast, iterable_name)
    return iterable_length
