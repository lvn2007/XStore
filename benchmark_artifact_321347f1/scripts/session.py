import json
import os

def save(session_data, filename="session.json"):
    if os.path.exists(filename):
        os.unlink(filename)
    json.dump(session_data, open(filename, "w"))
    
def load(filename="session.json"):
    return json.load(open(filename, "r"))

def clear(filename="session.json"):
    if os.path.exists(filename):
        os.unlink(filename)

__all__ = ["save", "load", "clear"]