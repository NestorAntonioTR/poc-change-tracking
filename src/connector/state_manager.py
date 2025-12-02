
import os
import sys
import logging
import json
from datetime import datetime
from typing import Dict, Any

from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('replication.log')
    ]
)
logger = logging.getLogger('sql_to_bq_replicator')

# Load environment variables
load_dotenv()


class StateManager:
    """Manages the replication state for tracking sync versions."""
    
    def __init__(self, state_file: str = 'replication_state.json'):
        self.state_file = state_file
        self.state = self._load_state()
    
    def _load_state(self) -> Dict[str, Dict[str, Any]]:
        """Load the replication state from the state file."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load state file: {e}")
                return {}
        else:
            return {}
    
    def _save_state(self):
        """Save the replication state to the state file."""
        try:
            with open(self.state_file, 'w') as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save state file: {e}")
    
    def get_last_sync_version(self, table_name: str) -> int:
        """Get the last sync version for a table."""
        table_state = self.state.get(table_name, {})
        return table_state.get('last_sync_version', 0)
    
    def update_sync_version(self, table_name: str, version: int):
        """Update the sync version for a table."""
        if table_name not in self.state:
            self.state[table_name] = {}
        
        self.state[table_name]['last_sync_version'] = version
        self.state[table_name]['last_sync_time'] = datetime.now().isoformat()
        
        self._save_state()
