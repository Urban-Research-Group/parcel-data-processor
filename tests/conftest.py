import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src", "processor"))

from .generate_truth import generate_truth

generate_truth()
