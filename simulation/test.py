import os
import sys
sys.path.append(os.getcwd())

print(os.getcwd())
print(sys.path)
print(sys.path)
from engine.match_engine import MatchEngine

me = MatchEngine(engine_id="test")

print(me)
