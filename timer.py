"""Python 3 Timer Class - Context Manager for Timing Code Blocks
Based on the Python 2 version from Corey Goldberg:
    * https://dzone.com/articles/python-timer-class-context
    * https://gist.github.com/cgoldberg/2942781
Example:
    import requests
    from timer import Timer
    url = 'https://github.com/rtogo'
    with Timer() as t:
        r = requests.get(url)
    
    print('Elapsed time: {} seconds'.format(t.elapsed))
Outputs:
    Elapsed time: 0.026 seconds
"""

from timeit import default_timer


class Timer(object):
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.timer = default_timer

    def __enter__(self):
        self.start = self.timer()
        return self

    def __exit__(self, *args):
        end = self.timer()
        self.elapsed_secs = end - self.start
        self.elapsed_msecs = self.elapsed_secs * 1000  # millisecs
        self.elapsed = '{:.3f}'.format(self.elapsed_secs)
        if self.verbose:
            print('elapsed time: {} seconds'.format(self.elapsed))
