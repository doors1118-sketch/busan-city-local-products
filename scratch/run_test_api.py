import os, sys
os.environ["CHATBOT_DB"] = "staging_chatbot_company.db"
sys.path.insert(0, ".")
import uvicorn
from api_server import app
uvicorn.run(app, host="127.0.0.1", port=8001)
