"""HDF5 entry using the identical new product and shared pairing protocol."""
from pathlib import Path
from .catalog_selector_pair import CatalogSelectorPairRunner, protocol_spec
if __name__=='__main__':
    CatalogSelectorPairRunner(protocol_spec(Path(__file__).resolve().parents[2],'hdf5'),'hdf5').main()
