"""
Environmental Units Registry for Water Quality Analysis

Auto-registers molecules as mass equivalents (e.g., O2 = 31.998 * gram).
Extendable to other environmental units like turbidity, pH, conductivity.
Pint handles all conversions and algebra automatically.
"""

import re
from pint import UnitRegistry


class EnvRegistry(UnitRegistry):
    """
    Enhanced Pint UnitRegistry with automatic environmental unit registration.
    
    Auto-registers molecules like O2 = 31.998 * gram, allowing Pint to handle
    all conversions and algebra automatically. Extensible to other environmental
    measurement units like turbidity (NTU), pH, conductivity (μS/cm).
    
    Example:
        ureg = EnvRegistry()
        oxygen = 5 * ureg.O2              # Auto-registers O2 = 31.998 * gram
        mass = oxygen.to('gram')          # 159.99 gram
        conc = oxygen / ureg.kg           # 5 O2/kg 
        mass_conc = conc.to('gram/kg')    # 159.99 gram/kg
    """
    
    # Atomic weights from NIST (2019 values)
    ATOMIC_WEIGHTS = {
        'H': 1.008, 'He': 4.0026, 'Li': 6.94, 'Be': 9.0122, 'B': 10.81,
        'C': 12.011, 'N': 14.007, 'O': 15.999, 'F': 18.998, 'Ne': 20.180,
        'Na': 22.990, 'Mg': 24.305, 'Al': 26.982, 'Si': 28.085, 'P': 30.974,
        'S': 32.06, 'Cl': 35.45, 'Ar': 39.948, 'K': 39.098, 'Ca': 40.078,
        'Sc': 44.956, 'Ti': 47.867, 'V': 50.942, 'Cr': 51.996, 'Mn': 54.938,
        'Fe': 55.845, 'Co': 58.933, 'Ni': 58.693, 'Cu': 63.546, 'Zn': 65.38,
        'Ga': 69.723, 'Ge': 72.630, 'As': 74.922, 'Se': 78.971, 'Br': 79.904,
        'Kr': 83.798, 'Rb': 85.468, 'Sr': 87.62, 'Y': 88.906, 'Zr': 91.224,
        'Nb': 92.906, 'Mo': 95.95, 'Tc': 98.0, 'Ru': 101.07, 'Rh': 102.906,
        'Pd': 106.42, 'Ag': 107.868, 'Cd': 112.414, 'In': 114.818, 'Sn': 118.710,
        'Sb': 121.760, 'Te': 127.60, 'I': 126.904, 'Xe': 131.293, 'Cs': 132.905,
        'Ba': 137.327, 'La': 138.905, 'Ce': 140.116, 'Pr': 140.908, 'Nd': 144.242,
        'Pm': 145.0, 'Sm': 150.36, 'Eu': 151.964, 'Gd': 157.25, 'Tb': 158.925,
        'Dy': 162.500, 'Ho': 164.930, 'Er': 167.259, 'Tm': 168.934, 'Yb': 173.045,
        'Lu': 174.967, 'Hf': 178.49, 'Ta': 180.948, 'W': 183.84, 'Re': 186.207,
        'Os': 190.23, 'Ir': 192.217, 'Pt': 195.084, 'Au': 196.967, 'Hg': 200.592,
        'Tl': 204.383, 'Pb': 207.2, 'Bi': 208.980, 'Po': 209.0, 'At': 210.0,
        'Rn': 222.0, 'Fr': 223.0, 'Ra': 226.0, 'Ac': 227.0, 'Th': 232.038,
        'Pa': 231.036, 'U': 238.029, 'Np': 237.0, 'Pu': 244.0, 'Am': 243.0
    }

    def __init__(self, *args, **kwargs):
        # Extract our custom parameters before calling super()
        self._debug = kwargs.pop('debug', False)
        
        super().__init__(*args, **kwargs)
        self.molecular_weights = {}
        
    def __getattr__(self, name):
        """Intercept attribute access to auto-register molecules"""
        try:
            # Try normal unit lookup first
            return super().__getattr__(name)
        except AttributeError:
            # Try to auto-register as molecule (will fail if not valid formula)
            try:
                return self._auto_register_molecule(name)
            except (ValueError, AttributeError):
                raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
    

    
    def _auto_register_molecule(self, formula):
        """Auto-register molecule as mass equivalent (e.g., O2 = 31.998 * gram)"""
        try:
            mw = self._calculate_molecular_weight(formula)
            self.define(f"{formula} = {mw} * gram")
            self.molecular_weights[formula] = mw
            
            if self._debug:
                print(f"Auto-registered {formula} = {mw:.3f} * gram")
            
            return super().__getattr__(formula)
            
        except Exception as e:
            raise AttributeError(f"Cannot auto-register {formula}: {e}")
    
    def _calculate_molecular_weight(self, formula):
        """Calculate molecular weight from chemical formula (e.g., 'H2O' -> 18.015)"""
        pattern = r'([A-Z][a-z]?)(\d*)'
        matches = re.findall(pattern, formula)
        
        if not matches:
            raise ValueError(f"Invalid chemical formula: {formula}")
        
        total_weight = 0.0
        for element, count in matches:
            if element not in self.ATOMIC_WEIGHTS:
                raise ValueError(f"Unknown element: {element}")
            count = int(count) if count else 1
            total_weight += self.ATOMIC_WEIGHTS[element] * count
        
        return total_weight
    
    # Pint handles all conversions automatically with mass equivalence definitions!
    
    def get_molecular_weight(self, molecule):
        """Get molecular weight for a molecule (auto-registers if needed)"""
        molecule_str = str(molecule)
        if molecule_str in self.molecular_weights:
            return self.molecular_weights[molecule_str]
        
        try:
            self._auto_register_molecule(molecule_str)
            return self.molecular_weights[molecule_str]
        except (ValueError, AttributeError):
            raise ValueError(f"Unknown molecule: {molecule_str}")
    
    def list_molecules(self):
        """List all registered molecules and their molecular weights"""
        return dict(self.molecular_weights)


# Default environmental registry instance
ureg = EnvRegistry()