"""HTPI Encounters Service - Manages patient encounters and visits"""

import os
import asyncio
import json
import logging
from datetime import datetime, timedelta
import nats
from nats.aio.client import Client as NATS
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
NATS_URL = os.environ.get('NATS_URL', 'nats://localhost:4222')
NATS_USER = os.environ.get('NATS_USER')
NATS_PASS = os.environ.get('NATS_PASSWORD')

# Mock encounters database (in production, would use MongoDB)
MOCK_ENCOUNTERS = {
    'enc-001': {
        'id': 'enc-001',
        'tenantId': 'tenant-001',
        'patientId': 'pat-001',
        'date': '2024-07-01T10:00:00Z',
        'type': 'office_visit',
        'provider': 'Dr. Smith',
        'chief_complaint': 'Annual checkup',
        'diagnoses': ['Z00.00'],
        'procedures': ['99213'],
        'vitals': {
            'blood_pressure': '120/80',
            'temperature': '98.6',
            'pulse': '72',
            'weight': '165 lbs'
        },
        'notes': 'Patient in good health. Continue current medications.',
        'status': 'completed',
        'created_at': '2024-07-01T10:00:00Z'
    },
    'enc-002': {
        'id': 'enc-002',
        'tenantId': 'tenant-001',
        'patientId': 'pat-002',
        'date': '2024-07-02T14:30:00Z',
        'type': 'follow_up',
        'provider': 'Dr. Johnson',
        'chief_complaint': 'Follow-up for hypertension',
        'diagnoses': ['I10'],
        'procedures': ['99214'],
        'vitals': {
            'blood_pressure': '145/90',
            'temperature': '98.2',
            'pulse': '78',
            'weight': '180 lbs'
        },
        'notes': 'Blood pressure slightly elevated. Adjusted medication dosage.',
        'status': 'completed',
        'created_at': '2024-07-02T14:30:00Z'
    }
}

class EncountersService:
    def __init__(self):
        self.nc = None
        
    async def connect(self):
        """Connect to NATS"""
        try:
            # Build connection options
            options = {
                'servers': [NATS_URL],
                'name': 'htpi-encounters-service',
                'reconnect_time_wait': 2,
                'max_reconnect_attempts': -1
            }
            if NATS_USER and NATS_PASS:
                options['user'] = NATS_USER
                options['password'] = NATS_PASS
            
            self.nc = await nats.connect(**options)
            logger.info(f"Connected to NATS at {NATS_URL}")
            
            # Subscribe to encounter requests
            await self.nc.subscribe("htpi.encounters.create", cb=self.handle_create_encounter)
            await self.nc.subscribe("htpi.encounters.update", cb=self.handle_update_encounter)
            await self.nc.subscribe("htpi.encounters.list", cb=self.handle_list_encounters)
            await self.nc.subscribe("htpi.encounters.get", cb=self.handle_get_encounter)
            await self.nc.subscribe("htpi.encounters.list.for.patient", cb=self.handle_list_patient_encounters)
            
            # Subscribe to health check requests
            await self.nc.subscribe("health.check", cb=self.handle_health_check)
            await self.nc.subscribe("htpi-encounters-service.health", cb=self.handle_health_check)
            
            logger.info("Encounters service subscriptions established")
        except Exception as e:
            logger.error(f"Failed to connect to NATS: {str(e)}")
            raise
    
    async def handle_create_encounter(self, msg):
        """Handle encounter creation"""
        try:
            data = json.loads(msg.data.decode())
            client_id = data.get('clientId')
            portal = data.get('portal', 'admin')
            
            # Create new encounter
            encounter_id = f"enc-{len(MOCK_ENCOUNTERS) + 1:03d}"
            new_encounter = {
                'id': encounter_id,
                'tenantId': data.get('tenantId'),
                'patientId': data.get('patientId'),
                'date': data.get('date', datetime.utcnow().isoformat()),
                'type': data.get('type'),
                'provider': data.get('provider'),
                'chief_complaint': data.get('chief_complaint'),
                'diagnoses': data.get('diagnoses', []),
                'procedures': data.get('procedures', []),
                'vitals': data.get('vitals', {}),
                'notes': data.get('notes', ''),
                'status': 'in_progress',
                'created_at': datetime.utcnow().isoformat()
            }
            
            # Add to mock database
            MOCK_ENCOUNTERS[encounter_id] = new_encounter
            
            # Send response
            channel = f"{portal}.encounters.response.{client_id}"
            await self.nc.publish(channel, 
                json.dumps({
                    'success': True,
                    'encounter': new_encounter,
                    'clientId': client_id
                }).encode())
            
            # Broadcast update
            await self.nc.publish(f"{portal}.broadcast.encounters.{data.get('tenantId')}", 
                json.dumps({
                    'action': 'created',
                    'encounter': new_encounter
                }).encode())
            
            logger.info(f"Created encounter: {encounter_id}")
            
        except Exception as e:
            logger.error(f"Error in handle_create_encounter: {str(e)}")
    
    async def handle_update_encounter(self, msg):
        """Handle encounter updates"""
        try:
            data = json.loads(msg.data.decode())
            encounter_id = data.get('encounterId')
            client_id = data.get('clientId')
            portal = data.get('portal', 'admin')
            
            if encounter_id not in MOCK_ENCOUNTERS:
                channel = f"{portal}.encounters.response.{client_id}"
                await self.nc.publish(channel, 
                    json.dumps({
                        'success': False,
                        'error': 'Encounter not found',
                        'clientId': client_id
                    }).encode())
                return
            
            # Update encounter
            encounter = MOCK_ENCOUNTERS[encounter_id]
            
            # Update fields
            if 'diagnoses' in data:
                encounter['diagnoses'] = data['diagnoses']
            if 'procedures' in data:
                encounter['procedures'] = data['procedures']
            if 'vitals' in data:
                encounter['vitals'].update(data['vitals'])
            if 'notes' in data:
                encounter['notes'] = data['notes']
            if 'status' in data:
                encounter['status'] = data['status']
            
            encounter['updated_at'] = datetime.utcnow().isoformat()
            
            # Send response
            channel = f"{portal}.encounters.response.{client_id}"
            await self.nc.publish(channel, 
                json.dumps({
                    'success': True,
                    'encounter': encounter,
                    'clientId': client_id
                }).encode())
            
            # Broadcast update
            await self.nc.publish(f"{portal}.broadcast.encounters.{encounter['tenantId']}", 
                json.dumps({
                    'action': 'updated',
                    'encounter': encounter
                }).encode())
            
            logger.info(f"Updated encounter: {encounter_id}")
            
        except Exception as e:
            logger.error(f"Error in handle_update_encounter: {str(e)}")
    
    async def handle_list_encounters(self, msg):
        """Handle list encounters request"""
        try:
            data = json.loads(msg.data.decode())
            tenant_id = data.get('tenantId')
            client_id = data.get('clientId')
            portal = data.get('portal', 'admin')
            
            # Filter by tenant
            encounters = [enc for enc in MOCK_ENCOUNTERS.values() 
                         if enc['tenantId'] == tenant_id]
            
            # Sort by date descending
            encounters.sort(key=lambda x: x['date'], reverse=True)
            
            channel = f"{portal}.encounters.response.{client_id}"
            await self.nc.publish(channel, 
                json.dumps({
                    'success': True,
                    'encounters': encounters,
                    'clientId': client_id
                }).encode())
            
        except Exception as e:
            logger.error(f"Error in handle_list_encounters: {str(e)}")
    
    async def handle_list_patient_encounters(self, msg):
        """Handle list encounters for specific patient"""
        try:
            data = json.loads(msg.data.decode())
            patient_id = data.get('patientId')
            tenant_id = data.get('tenantId')
            client_id = data.get('clientId')
            portal = data.get('portal', 'customer')
            
            # Filter by patient and tenant
            encounters = [enc for enc in MOCK_ENCOUNTERS.values() 
                         if enc['patientId'] == patient_id and enc['tenantId'] == tenant_id]
            
            # Sort by date descending
            encounters.sort(key=lambda x: x['date'], reverse=True)
            
            channel = f"{portal}.encounters.response.{client_id}"
            await self.nc.publish(channel, 
                json.dumps({
                    'success': True,
                    'encounters': encounters,
                    'clientId': client_id
                }).encode())
            
        except Exception as e:
            logger.error(f"Error in handle_list_patient_encounters: {str(e)}")
    
    async def handle_get_encounter(self, msg):
        """Handle get single encounter request"""
        try:
            data = json.loads(msg.data.decode())
            encounter_id = data.get('encounterId')
            client_id = data.get('clientId')
            portal = data.get('portal', 'admin')
            
            encounter = MOCK_ENCOUNTERS.get(encounter_id)
            
            if not encounter:
                channel = f"{portal}.encounters.response.{client_id}"
                await self.nc.publish(channel, 
                    json.dumps({
                        'success': False,
                        'error': 'Encounter not found',
                        'clientId': client_id
                    }).encode())
                return
            
            channel = f"{portal}.encounters.response.{client_id}"
            await self.nc.publish(channel, 
                json.dumps({
                    'success': True,
                    'encounter': encounter,
                    'clientId': client_id
                }).encode())
            
        except Exception as e:
            logger.error(f"Error in handle_get_encounter: {str(e)}")
    
    async def handle_health_check(self, msg):
        """Handle health check requests"""
        try:
            data = json.loads(msg.data.decode())
            request_id = data.get('requestId')
            client_id = data.get('clientId')
            
            # Calculate uptime
            uptime = datetime.utcnow() - self.start_time if hasattr(self, 'start_time') else timedelta(0)
            
            health_response = {
                'serviceId': 'htpi-encounters-service',
                'status': 'healthy',
                'message': 'Encounters service operational',
                'version': '1.0.0',
                'uptime': str(uptime),
                'requestId': request_id,
                'clientId': client_id,
                'timestamp': datetime.utcnow().isoformat(),
                'stats': {
                    'total_encounters': len(MOCK_ENCOUNTERS),
                    'encounters_today': len([e for e in MOCK_ENCOUNTERS.values() 
                                           if e['date'].startswith(datetime.utcnow().date().isoformat())])
                }
            }
            
            # Send response back to admin portal
            await self.nc.publish(f"admin.health.response.{client_id}", 
                                json.dumps(health_response).encode())
            
            logger.info(f"Health check response sent for request {request_id}")
            
        except Exception as e:
            logger.error(f"Error handling health check: {str(e)}")
    
    async def run(self):
        """Run the service"""
        self.start_time = datetime.utcnow()
        
        await self.connect()
        logger.info("Encounters service is running...")
        
        # Keep service running
        try:
            await asyncio.Future()  # Run forever
        except KeyboardInterrupt:
            pass
        finally:
            await self.nc.close()

async def main():
    """Main entry point"""
    service = EncountersService()
    await service.run()

if __name__ == '__main__':
    asyncio.run(main())