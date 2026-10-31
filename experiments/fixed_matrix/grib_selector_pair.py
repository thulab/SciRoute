"""GRIB entry: format travels through the supervisor via this fixed module."""
from pathlib import Path
from .catalog_selector_pair import CatalogSelectorPairRunner, protocol_spec
if __name__=='__main__':
    CatalogSelectorPairRunner(protocol_spec(Path(__file__).resolve().parents[2],'grib'),'grib').main()
