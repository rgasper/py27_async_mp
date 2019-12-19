from async_main import pipeline
import unittest

class TestAsyncPipeline(unittest.TestCase):
    ''' very basic tests, just to see if they finish
    pipeline will not complete unless all data elements are processed '''
    
    def test_small_one_worker(self):
        pipeline(10,1)
        self.assertTrue(True)

    def test_small_two_workers(self):
        pipeline(10,2)
        self.assertTrue(True)

    def test_zero(self):
        with self.assertRaises(AssertionError):
            pipeline(0,0)

    def test_medium(self):
        pipeline(1000, 100)
        self.assertTrue(True)

    def test_large(self):
        pipeline(100000, 1000)
        self.assertTrue(True)

if __name__ == "__main__":
    unittest.main()