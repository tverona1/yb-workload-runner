import time

class TimingWrapper:
    def __init__(self, environment, stub, request_type = None):
        self.env = environment
        self._stub_class = stub.__class__
        self._stub = stub
        self._request_type = request_type

    def __getattr__(self, name):
        func = self._stub_class.__getattribute__(self._stub, name)

        def wrapper(*args, **kwargs):
            request_meta = {
                "request_type": self._request_type,
                "name": name,
                "start_time": time.time(),
                "response_length": 0,
                "exception": None,
                "context": None,
                "response": None,
            }
            start_perf_counter = time.perf_counter()
            ret = func(*args, **kwargs)
            request_meta["response_time"] = (time.perf_counter() - start_perf_counter) * 1000
            self.env.events.request.fire(**request_meta)
            return ret, request_meta["response_time"]
        return wrapper
