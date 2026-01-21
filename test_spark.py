#!/usr/bin/env python
"""Quick test to verify Spark/Django consumer works"""
import sys

sys.path.insert(0, "Django-Dashboard")

try:
    print("Testing lazy imports...")
    from dashboard.consumer_user import classify_text

    print("[OK] Successfully imported classify_text (lazy initialization works)")

    print("\nTesting classification (this will initialize Spark)...")
    result = classify_text("I am very happy today!")
    print(f"[OK] Classification result: {result}")

except Exception as e:
    print(f"[ERROR] {type(e).__name__}: {e}")
    import traceback

    traceback.print_exc()
    sys.exit(1)

print("\n[OK] All tests passed!")
