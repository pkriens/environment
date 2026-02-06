from enum import Enum

class AuthorityType(Enum):
    def __init__(self, name, description=None):
        self.id = self._name_
        self.full_name = name
        self.description = description
    
    WB = ("waterbeheerder", "Regional water authority responsible for water management")
    PR = ("provincie", "Provincial level authority") 
    NL = ("nederland", "National level authority")
    SD = ("stroomgebieddistricten", "Basin level authority covering multiple regions")

class Authority(Enum):
    def __init__(self, full_name, authority_type: AuthorityType, description=None):
        self.code = self._name_  # Use the enum name as the code
        self.full_name = full_name
        self.authority_type = authority_type        
        self.description = description

    # National level (Nederland)
    NL = ("Nederland", AuthorityType.NL)

    # Provincial level (Provincie)
    DRNT = ("Drenthe", AuthorityType.PR)
    FLVL = ("Flevoland", AuthorityType.PR)
    FRSL = ("Fryslân", AuthorityType.PR)
    GLDR = ("Gelderland", AuthorityType.PR)
    GRNN = ("Groningen", AuthorityType.PR)
    LMBR = ("Limburg", AuthorityType.PR)
    NBRB = ("Noord-Brabant", AuthorityType.PR)
    NHLL = ("Noord-Holland", AuthorityType.PR)
    OVRS = ("Overijssel", AuthorityType.PR)
    UTRC = ("Utrecht", AuthorityType.PR)
    ZLND = ("Zeeland", AuthorityType.PR)
    ZHLL = ("Zuid-Holland", AuthorityType.PR)

    # Stream basin districts (Stroomgebieddistricten)
    EEMS = ("Eems", AuthorityType.SD)
    MAAS = ("Maas", AuthorityType.SD)
    RIJN = ("Rijn", AuthorityType.SD)
    SCHL = ("Schelde", AuthorityType.SD)

    # Water authorities (Waterbeheerders/Hoogheemraadschappen)
    HHSR = ("Hoogheemraadschap De Stichtse Rijnlanden", AuthorityType.WB)
    HHHN = ("Hoogheemraadschap Hollands Noorderkwartier", AuthorityType.WB)
    HHDL = ("Hoogheemraadschap van Delfland", AuthorityType.WB)
    HHRN = ("Hoogheemraadschap van Rijnland", AuthorityType.WB)
    HHSK = ("Hoogheemraadschap van Schieland en Krimpenerwaard", AuthorityType.WB)
    RWS = ("Rijkswaterstaat", AuthorityType.WB)
    WSAM = ("Waterschap Aa en Maas", AuthorityType.WB)
    WSAG = ("Waterschap Amstel Gooi en Vecht", AuthorityType.WB)
    WSBD = ("Waterschap Brabantse Delta", AuthorityType.WB)
    WSDM = ("Waterschap De Dommel", AuthorityType.WB)
    WSDO = ("Waterschap Drents Overijsselse Delta", AuthorityType.WB)
    WSHD = ("Waterschap Hollandse Delta", AuthorityType.WB)
    WSHA = ("Waterschap Hunze en Aa's", AuthorityType.WB)
    WSLM = ("Waterschap Limburg", AuthorityType.WB)
    WSNZ = ("Waterschap Noorderzijlvest", AuthorityType.WB)
    WSRY = ("Waterschap Rijn en IJssel", AuthorityType.WB)
    WSRV = ("Waterschap Rivierenland", AuthorityType.WB)
    WSSC = ("Waterschap Scheldestromen", AuthorityType.WB)
    WSVV = ("Waterschap Vallei en Veluwe", AuthorityType.WB)
    WSVC = ("Waterschap Vechtstromen", AuthorityType.WB)
    WSZZ = ("Waterschap Zuiderzeeland", AuthorityType.WB)
    WFRY = ("Wetterskip Fryslân", AuthorityType.WB)

    @classmethod
    def by_name(cls, name: str):
        """Find authority by exact name match"""
        for authority in cls:
            if authority.full_name == name:
                return authority
        return None

    @classmethod
    def by_type(cls, authority_type: AuthorityType):
        """Get all authorities of a specific type"""
        return [auth for auth in cls if auth.authority_type == authority_type]
