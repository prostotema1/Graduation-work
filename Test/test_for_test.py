import unittest

from Test.generator_test import Tests


repeat_count = 100
success = 0
for _ in range(100):
    test_suite = unittest.TestLoader().loadTestsFromTestCase(Tests)
    run = unittest.TextTestRunner()
    result = run.run(test_suite)
    if not result.wasSuccessful():
        print("иди дебаж")
    else:
        success += 1
print(success)