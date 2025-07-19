import pandas as pd

def load_csv(input_csv):
    try:
        df = pd.read_csv(input_csv)
        return df
    except FileNotFoundError:
        print(f"[Error] File not found: {input_csv}")

# import pandas as pd

# def load_csv(input_csv):
#     try:
#         df = pd.read_csv(input_csv)
#         return df
#     except FileNotFoundError:
#         print(f"[Error] File not found: {input_csv}")
#     except pd.errors.ParserError:
#         print(f"[Error] Failed to parse CSV file: {input_csv}")
#     except Exception as e:
#         print(f"[Error] Unexpected error: {e}")
