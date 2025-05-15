#!/usr/bin/env python
# -*- coding: utf-8 -*-

import subprocess
import os
import time
import signal
import sys
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('pipeline-starter')

# Define paths
CURRENT_DIR = Path(__file__).parent.absolute()
PROJECT_ROOT = CURRENT_DIR.parent
NOW_PLAYING_STREAMER_PATH = PROJECT_ROOT / "now-playing-streamer" / "main"

class PipelineStarter:
    """
    Utility to start all components of the Twitter data processing pipeline.
    """
    
    def __init__(self):
        """Initialize the pipeline starter."""
        self.processes = {}
        logger.info("Pipeline starter initialized")
        
    def start_kafka_topic(self):
        """Create Kafka topic if it doesn't exist."""
        logger.info("Creating Kafka topic...")
        try:
            subprocess.run(
                ["python", str(CURRENT_DIR / "kafka_utils.py")],
                check=True
            )
            logger.info("Kafka topic created successfully")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to create Kafka topic: {str(e)}")
            return False
        return True
        
    def start_now_playing_streamer(self):
        """Start the now-playing-streamer."""
        logger.info("Starting now-playing-streamer...")
        try:
            process = subprocess.Popen(
                ["python", str(NOW_PLAYING_STREAMER_PATH / "main.py")],
                cwd=str(NOW_PLAYING_STREAMER_PATH),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            self.processes['now_playing_streamer'] = process
            logger.info("now-playing-streamer started")
            # Wait a bit to ensure the server is up
            time.sleep(3)
        except Exception as e:
            logger.error(f"Failed to start now-playing-streamer: {str(e)}")
            return False
        return True
        
    def start_twitter_stream_connector(self):
        """Start the Twitter stream connector."""
        logger.info("Starting Twitter stream connector...")
        try:
            process = subprocess.Popen(
                ["python", str(CURRENT_DIR / "twitter_stream_connector.py")],
                cwd=str(CURRENT_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            self.processes['twitter_stream_connector'] = process
            logger.info("Twitter stream connector started")
            # Wait a bit to ensure the connector is up
            time.sleep(2)
        except Exception as e:
            logger.error(f"Failed to start Twitter stream connector: {str(e)}")
            return False
        return True
        
    def start_spark_streaming(self):
        """Start the Spark streaming application."""
        logger.info("Starting Spark streaming application...")
        try:
            process = subprocess.Popen(
                ["python", str(CURRENT_DIR / "spark_streaming.py")],
                cwd=str(CURRENT_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            self.processes['spark_streaming'] = process
            logger.info("Spark streaming application started")
        except Exception as e:
            logger.error(f"Failed to start Spark streaming application: {str(e)}")
            return False
        return True
        
    def start_all(self):
        """Start all components of the pipeline."""
        logger.info("Starting all pipeline components...")
        
        # Create Kafka topic
        if not self.start_kafka_topic():
            logger.error("Failed to create Kafka topic, but continuing...")
        
        # Start now-playing-streamer
        if not self.start_now_playing_streamer():
            logger.error("Failed to start now-playing-streamer")
            self.stop_all()
            return False
        
        # Start Twitter stream connector
        if not self.start_twitter_stream_connector():
            logger.error("Failed to start Twitter stream connector")
            self.stop_all()
            return False
        
        # Start Spark streaming application
        if not self.start_spark_streaming():
            logger.error("Failed to start Spark streaming application")
            self.stop_all()
            return False
        
        logger.info("All pipeline components started successfully")
        return True
        
    def stop_all(self):
        """Stop all running processes."""
        logger.info("Stopping all processes...")
        for name, process in self.processes.items():
            try:
                logger.info(f"Stopping {name}...")
                process.terminate()
                process.wait(timeout=5)
                logger.info(f"{name} stopped")
            except Exception as e:
                logger.error(f"Error stopping {name}: {str(e)}")
                # Force kill if terminate doesn't work
                try:
                    process.kill()
                except:
                    pass
        
        self.processes = {}
        logger.info("All processes stopped")
        
    def monitor_processes(self):
        """Monitor running processes and log their output."""
        try:
            while self.processes:
                for name, process in list(self.processes.items()):
                    # Check if process is still running
                    if process.poll() is not None:
                        logger.warning(f"{name} exited with code {process.returncode}")
                        # Read any remaining output
                        stdout, stderr = process.communicate()
                        if stdout:
                            logger.info(f"{name} stdout: {stdout}")
                        if stderr:
                            logger.error(f"{name} stderr: {stderr}")
                        # Remove from processes dict
                        del self.processes[name]
                    else:
                        # Read output without blocking
                        stdout = process.stdout.readline() if process.stdout else ""
                        stderr = process.stderr.readline() if process.stderr else ""
                        if stdout:
                            logger.info(f"{name}: {stdout.strip()}")
                        if stderr:
                            logger.error(f"{name}: {stderr.strip()}")
                
                # Sleep a bit to avoid high CPU usage
                time.sleep(0.1)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, stopping all processes...")
        finally:
            self.stop_all()

def main():
    """Main function to start the pipeline."""
    starter = PipelineStarter()
    
    # Register signal handlers
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, stopping all processes...")
        starter.stop_all()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start all components
    if starter.start_all():
        logger.info("Pipeline started successfully")
        # Monitor processes
        starter.monitor_processes()
    else:
        logger.error("Failed to start pipeline")
        sys.exit(1)

if __name__ == "__main__":
    main()
