#!/usr/bin/env python3
"""
Checkpoint-based Batch Processor for IoT Data
Processes new files since last checkpoint every 5 minutes
"""

import os
import json
import glob
import pandas as pd
from datetime import datetime, timezone, timedelta
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CheckpointProcessor:
    def __init__(self, 
                 landing_zone="data/landing_zone",
                 checkpoint_file="data/checkpoints/last_processed.json",
                 processed_dir="data/processed"):
        
        self.landing_zone = landing_zone
        self.checkpoint_file = checkpoint_file
        self.processed_dir = processed_dir
        
        # Ensure directories exist
        os.makedirs(os.path.dirname(checkpoint_file), exist_ok=True)
        os.makedirs(processed_dir, exist_ok=True)
        
        logger.info(f"🔄 Checkpoint Processor initialized")
        logger.info(f"📁 Landing zone: {landing_zone}")
        logger.info(f"💾 Checkpoint file: {checkpoint_file}")
        logger.info(f"📦 Processed directory: {processed_dir}")

    def load_checkpoint(self):
        """Load the last processed timestamp and processed files from checkpoint file"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    checkpoint_data = json.load(f)
                last_processed = datetime.fromisoformat(checkpoint_data['last_processed'])
                # Ensure timezone-aware datetime
                if last_processed.tzinfo is None:
                    last_processed = last_processed.replace(tzinfo=timezone.utc)
                processed_files = set(checkpoint_data.get('processed_files', []))
                logger.info(f"📅 Last processed: {last_processed}")
                logger.info(f"📁 Processed files: {len(processed_files)}")
                return last_processed, processed_files
            except Exception as e:
                logger.warning(f"⚠️  Error loading checkpoint: {e}")
                return None, set()
        else:
            logger.info("🆕 No checkpoint found, processing all files")
            return None, set()

    def save_checkpoint(self, timestamp, processed_files):
        """Save the current processing timestamp and processed files to checkpoint file"""
        checkpoint_data = {
            'last_processed': timestamp.isoformat(),
            'processed_files': list(processed_files),
            'checkpoint_created': datetime.now(timezone.utc).isoformat()
        }
        
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
            logger.info(f"💾 Checkpoint saved: {timestamp} with {len(processed_files)} processed files")
        except Exception as e:
            logger.error(f"❌ Error saving checkpoint: {e}")

    def get_new_files(self, since_timestamp=None, processed_files=None):
        """Get list of new CSV files since last checkpoint, excluding already processed files"""
        pattern = os.path.join(self.landing_zone, "iot_data_*.csv")
        all_files = glob.glob(pattern)
        
        if processed_files is None:
            processed_files = set()
        
        # Filter out already processed files
        unprocessed_files = []
        for file_path in all_files:
            file_name = os.path.basename(file_path)
            if file_name not in processed_files:
                unprocessed_files.append(file_path)
        
        if since_timestamp is None:
            # Process all unprocessed files if no timestamp checkpoint
            new_files = unprocessed_files
        else:
            # Filter files modified after checkpoint
            new_files = []
            for file_path in unprocessed_files:
                file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path), tz=timezone.utc)
                if file_mtime > since_timestamp:
                    new_files.append(file_path)
        
        # Sort by modification time
        new_files.sort(key=lambda x: os.path.getmtime(x))
        
        logger.info(f"📂 Found {len(new_files)} new files to process (skipped {len(processed_files)} already processed)")
        return new_files

    def process_files(self, file_paths):
        """Process a batch of CSV files and combine them"""
        if not file_paths:
            logger.info("📭 No files to process")
            return None
        
        combined_data = []
        processed_files = []
        
        for file_path in file_paths:
            try:
                # Read CSV file
                df = pd.read_csv(file_path)
                
                # Add processing metadata
                df['source_file'] = os.path.basename(file_path)
                df['processing_timestamp'] = datetime.now().isoformat()
                
                combined_data.append(df)
                processed_files.append(file_path)
                
                logger.info(f"✅ Processed: {os.path.basename(file_path)} ({len(df)} records)")
                
            except Exception as e:
                logger.error(f"❌ Error processing {file_path}: {e}")
        
        if combined_data:
            # Combine all data
            combined_df = pd.concat(combined_data, ignore_index=True)
            
            # Generate batch filename
            batch_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            batch_filename = f"iot_batch_{batch_timestamp}.csv"
            batch_path = os.path.join(self.processed_dir, batch_filename)
            
            # Save combined batch
            combined_df.to_csv(batch_path, index=False)
            
            logger.info(f"📦 Batch saved: {batch_filename} ({len(combined_df)} total records)")
            
            # Move processed files to archive
            self.archive_files(processed_files)
            
            return batch_path, len(combined_df)
        
        return None, 0

    def archive_files(self, file_paths):
        """Move processed files to archive directory"""
        archive_dir = os.path.join(self.processed_dir, "archived")
        os.makedirs(archive_dir, exist_ok=True)
        
        for file_path in file_paths:
            try:
                filename = os.path.basename(file_path)
                archive_path = os.path.join(archive_dir, filename)
                
                # Move file to archive
                os.rename(file_path, archive_path)
                logger.info(f"📁 Archived: {filename}")
                
            except Exception as e:
                logger.error(f"❌ Error archiving {file_path}: {e}")

    def run_batch_processing(self):
        """Run one batch processing cycle with file-level checkpointing"""
        logger.info("🔄 Starting batch processing cycle...")
        
        # Load checkpoint
        last_processed, processed_files = self.load_checkpoint()
        
        # Get new files (excluding already processed ones)
        new_files = self.get_new_files(last_processed, processed_files)
        
        if not new_files:
            logger.info("📭 No new files to process")
            return None, 0
        
        # Process files
        batch_path, record_count = self.process_files(new_files)
        
        if batch_path:
            # Update processed files set
            new_processed_files = processed_files.copy()
            for file_path in new_files:
                file_name = os.path.basename(file_path)
                new_processed_files.add(file_name)
            
            # Update checkpoint
            current_time = datetime.now(timezone.utc)
            self.save_checkpoint(current_time, new_processed_files)
            
            logger.info(f"✅ Batch processing completed:")
            logger.info(f"   Files processed: {len(new_files)}")
            logger.info(f"   Records processed: {record_count}")
            logger.info(f"   Batch file: {os.path.basename(batch_path)}")
            logger.info(f"   Total processed files: {len(new_processed_files)}")
            
            return batch_path, record_count
        else:
            logger.warning("⚠️  No data was processed in this batch")
            return None, 0

def main():
    """Main function for checkpoint processing"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Checkpoint-based Batch Processor")
    parser.add_argument("--landing-zone", default="data/landing_zone", help="Landing zone directory")
    parser.add_argument("--checkpoint-file", default="data/checkpoints/last_processed.json", help="Checkpoint file path")
    parser.add_argument("--processed-dir", default="data/processed", help="Processed files directory")
    
    args = parser.parse_args()
    
    # Create and run processor
    processor = CheckpointProcessor(
        landing_zone=args.landing_zone,
        checkpoint_file=args.checkpoint_file,
        processed_dir=args.processed_dir
    )
    
    processor.run_batch_processing()

if __name__ == "__main__":
    main()
