from noalchemy import create_engine

noalchemy = create_engine("mongodb://localhost:27017/test", mock=False)
