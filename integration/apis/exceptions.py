class APIRateLimit(Exception):
    def __init__(self):
        self.message = f"API rate limit exceeded"
    
    def __str__(self):
        return self.message