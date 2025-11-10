class DetailEntity():
    def __init_subclass__(cls, *args, **kwargs):
        pass

    def __init__(self):
        self.phRecipe = None
        self.prUnitId = None
        self.maxEfctTime = None
        self.expireTime = None
