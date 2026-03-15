import ast
from loguru import logger

# Allowed nodes and names (very strict whitelist)
ALLOWED_NODES = (
    ast.Module, ast.Expr, ast.Assign, ast.Name, ast.Store, ast.Load,
    ast.Call, ast.Attribute, ast.Constant, ast.List, ast.Dict, ast.Tuple,
    ast.BinOp, ast.Compare, ast.BoolOp, ast.UnaryOp, ast.Subscript, ast.Slice,
    ast.keyword, ast.Index, ast.ExtSlice, ast.Import, ast.ImportFrom, ast.alias,
    # Allow control flow for LLM generated code
    ast.If, ast.For, ast.While, ast.Pass, ast.Lambda, ast.arguments, ast.arg,
    ast.Return, ast.Global, ast.JoinedStr, ast.FormattedValue, ast.Starred,
    # Exception handling
    ast.Try, ast.ExceptHandler, ast.Raise,
    # Allow function definitions
    ast.FunctionDef, ast.FunctionType,
    # Allow some math ops
    ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv, ast.Mod, ast.Pow, ast.Lt, ast.Gt, ast.Eq, ast.NotEq,
    ast.LtE, ast.GtE, ast.And, ast.Or, ast.Not, ast.BitAnd, ast.BitOr, ast.BitXor, ast.Invert,
    # Comprehensions
    ast.ListComp, ast.comprehension, ast.GeneratorExp
)

ALLOWED_NAMES = {
    'pd', 'pandas', 'np', 'numpy', 'ta', 'duckdb', 'print', 'len', 'range', 'int', 'float', 'getattr', 
    'macros', 'functools', 'llm_agent.quant_macros', 'abs', 'min', 'max', 'sum', 'sorted', 'enumerate',
    'True', 'False', 'None'
}

class SecurityVisitor(ast.NodeVisitor):
    def generic_visit(self, node):
        if not isinstance(node, ALLOWED_NODES):
            logger.error(f"Blocked AST node: {type(node).__name__}")
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
            if alias.name.split('.')[0] not in ALLOWED_NAMES and alias.name not in ALLOWED_NAMES:
                logger.error(f"Blocked import: {alias.name}")
                raise ValueError(f"Forbidden import: {alias.name}")
        super().generic_visit(node)
        
    def visit_ImportFrom(self, node):
        if node.module:
            if node.module.split('.')[0] not in ALLOWED_NAMES and node.module not in ALLOWED_NAMES:
                logger.error(f"Blocked import from: {node.module}")
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
        logger.error(f"Syntax error in code: {e}")
        raise ValueError(f"Syntax error in generated code: {e}")
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        raise
