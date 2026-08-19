import io
import sys
import traceback

def execute_python_code(code_string: str) -> str:
    """Executes arbitrary Python code safely and captures printed outputs."""
    buffer = io.StringIO()
    sys.stdout = buffer
    
    # Muhalli mai tsafta na gudanar da code
    local_env = {}
    
    try:
        exec(code_string, {}, local_env)
        output = buffer.getvalue()
        if not output.strip():
            output = "✅ Code ran successfully with no visual output."
        return output.strip()
    except Exception as e:
        error_class, exception, tb = sys.exc_info()
        return f"❌ Execution Error:\n{traceback.format_exc()}"
    finally:
        sys.stdout = sys.__stdout__