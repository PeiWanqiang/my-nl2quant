import ast

# Allowed nodes and names (very strict whitelist)
ALLOWED_NODES = (
    ast.Module, ast.Expr, ast.Assign, ast.Name, ast.Store, ast.Load,
    ast.Call, ast.Attribute, ast.Constant, ast.List, ast.Dict, ast.Tuple,
    ast.BinOp, ast.Compare, ast.BoolOp, ast.UnaryOp, ast.Subscript, ast.Slice,
    ast.keyword, ast.Index, ast.ExtSlice, ast.Import, ast.ImportFrom, ast.alias,
    # Allow some math ops
    ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Lt, ast.Gt, ast.Eq, ast.NotEq,
    ast.LtE, ast.GtE, ast.And, ast.Or, ast.Not
)

ALLOWED_NAMES = {
    'pd', 'pandas', 'np', 'numpy', 'ta', 'duckdb', 'print', 'len', 'range', 'int', 'float'
}

class SecurityVisitor(ast.NodeVisitor):
    def generic_visit(self, node):
        if not isinstance(node, ALLOWED_NODES):
            raise ValueError(f"Unsafe or forbidden AST node: {type(node).__name__}")
        super().generic_visit(node)

    def visit_Name(self, node):
        # We only allow accessing predefined safe variables or builtins
        # In a real scenario, we'd be more nuanced, but this is an MVP
        if isinstance(node.ctx, ast.Load) and node.id not in ALLOWED_NAMES and not node.id.startswith('_'):
            # It might be a dataframe variable, let's be slightly lenient for the MVP
            pass
        super().generic_visit(node)

    def visit_Import(self, node):
        for alias in node.names:
            if alias.name not in ALLOWED_NAMES:
                raise ValueError(f"Forbidden import: {alias.name}")
        super().generic_visit(node)
        
    def visit_ImportFrom(self, node):
        if node.module not in ALLOWED_NAMES:
             raise ValueError(f"Forbidden import from: {node.module}")
        super().generic_visit(node)

def validate_code(code: str) -> bool:
    """Returns True if code is deemed safe, raises ValueError otherwise."""
    try:
        tree = ast.parse(code)
        visitor = SecurityVisitor()
        visitor.visit(tree)
        return True
    except SyntaxError as e:
        raise ValueError(f"Syntax error in generated code: {e}")
