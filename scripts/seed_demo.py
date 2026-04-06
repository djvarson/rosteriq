#!/usr/bin/env python3
"""
RosterIQ Demo Data Seeding Script

Creates a complete demo environment with:
- A sample pub venue (The Royal Oak, Fitzroy VIC)
- Demo user account
- 12 diverse staff members with realistic Aussie names
- 2 weeks of forecast data with realistic demand patterns
- Sample POS transaction data
- Location data for competitor and event analysis

Run after database setup:
    python scripts/seed_demo.py
"""

import os
import sys
import json
from datetime import datetime, timedelta
from typing import Optional
import hashlib

# Add parent directory to path to import rosteriq modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import psycopg2
    from psycopg2.extras import execute_values
    import jwt
except ImportError:
    print("Error: Required packages not found. Install with: pip install -r requirements.txt")
    sys.exit(1)


class RosterIQSeeder:
    """Seeds RosterIQ database with demo data"""

    def __init__(self):
        """Initialize database connection"""
        db_url = os.getenv('DATABASE_URL', 'postgresql://rosteriq:rosteriq@localhost:5432/rosteriq')

        try:
            self.conn = psycopg2.connect(db_url)
            self.cursor = self.conn.cursor()
            print("Connected to database")
        except psycopg2.Error as e:
            print(f"Error connecting to database: {e}")
            sys.exit(1)

    def close(self):
        """Close database connection"""
        self.cursor.close()
        self.conn.close()

    def seed_venue(self) -> int:
        """
        Create demo venue: The Royal Oak, Fitzroy VIC
        Returns venue_id
        """
        print("\nCreating demo venue...")

        venue_data = {
            'name': 'The Royal Oak',
            'location': 'Fitzroy VIC 3065',
            'address': '123 Brunswick Street, Fitzroy VIC 3065, Australia',
            'latitude': -37.7991,  # Fitzroy coordinates
            'longitude': 144.9760,
            'phone': '(03) 9417 5000',
            'email': 'contact@theroyaloak.com.au',
            'timezone': 'Australia/Melbourne',
            'cuisine_type': 'Pub',
            'capacity': 150,
            'opening_time': '10:00',  # Opens 10am
            'closing_time': '23:00',  # Closes 11pm
            'description': 'Iconic Melbourne pub with craft beers and classic pub food',
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow(),
        }

        self.cursor.execute("""
            INSERT INTO venues (
                name, location, address, latitude, longitude,
                phone, email, timezone, cuisine_type, capacity,
                opening_time, closing_time, description,
                created_at, updated_at
            ) VALUES (
                %(name)s, %(location)s, %(address)s, %(latitude)s, %(longitude)s,
                %(phone)s, %(email)s, %(timezone)s, %(cuisine_type)s, %(capacity)s,
                %(opening_time)s, %(closing_time)s, %(description)s,
                %(created_at)s, %(updated_at)s
            )
            RETURNING id
        """, venue_data)

        venue_id = self.cursor.fetchone()[0]
        self.conn.commit()
        print(f"Created venue: {venue_data['name']} (ID: {venue_id})")
        return venue_id

    def seed_user(self, venue_id: int) -> tuple:
        """
        Create demo user account
        Returns (user_id, jwt_token)
        """
        print("\nCreating demo user...")

        email = 'demo@rosteriq.com'
        password = 'demo123'

        # Hash password using bcrypt
        import passlib.context
        pwd_context = passlib.context.CryptContext(schemes=["bcrypt"], deprecated="auto")
        hashed_password = pwd_context.hash(password)

        user_data = {
            'venue_id': venue_id,
            'email': email,
            'password_hash': hashed_password,
            'full_name': 'Demo User',
            'role': 'manager',  # Full access for demo
            'is_active': True,
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow(),
        }

        self.cursor.execute("""
            INSERT INTO users (
                venue_id, email, password_hash, full_name, role,
                is_active, created_at, updated_at
            ) VALUES (
                %(venue_id)s, %(email)s, %(password_hash)s, %(full_name)s,
                %(role)s, %(is_active)s, %(created_at)s, %(updated_at)s
            )
            RETURNING id
        """, user_data)

        user_id = self.cursor.fetchone()[0]
        self.conn.commit()

        # Generate JWT token for immediate use
        jwt_secret = os.getenv('RIQ_JWT_SECRET', 'dev-secret-key-change-in-production')
        token_payload = {
            'sub': str(user_id),
            'venue_id': venue_id,
            'email': email,
            'role': 'manager',
            'iat': datetime.utcnow(),
            'exp': datetime.utcnow() + timedelta(days=30),
        }
        jwt_token = jwt.encode(token_payload, jwt_secret, algorithm='HS256')

        print(f"Created user: {email}")
        print(f"Password: {password}")
        return user_id, jwt_token

    def seed_staff(self, venue_id: int) -> list:
        """
        Create 12 demo employees with realistic Australian names
        Returns list of staff_ids
        """
        print("\nCreating demo staff...")

        # Realistic Australian pub staff with diverse names and experience
        staff_list = [
            {'name': 'James Mitchell', 'role': 'Bar Manager', 'award_level': 3},
            {'name': 'Sarah O\'Brien', 'role': 'Head Chef', 'award_level': 3},
            {'name': 'Lucas Chen', 'role': 'Bartender', 'award_level': 2},
            {'name': 'Emma Thompson', 'role': 'Waitstaff', 'award_level': 2},
            {'name': 'Marcus Johnson', 'role': 'Kitchen Hand', 'award_level': 1},
            {'name': 'Sophie Williams', 'role': 'Waitstaff', 'award_level': 2},
            {'name': 'Daniel Rodriguez', 'role': 'Bartender', 'award_level': 2},
            {'name': 'Isabella Brown', 'role': 'Host', 'award_level': 1},
            {'name': 'Aisha Patel', 'role': 'Floor Manager', 'award_level': 3},
            {'name': 'Oliver Green', 'role': 'Kitchen Hand', 'award_level': 1},
            {'name': 'Mia Davies', 'role': 'Waitstaff', 'award_level': 2},
            {'name': 'Liam Walsh', 'role': 'Bartender', 'award_level': 2},
        ]

        staff_ids = []
        for staff in staff_list:
            staff_data = {
                'venue_id': venue_id,
                'name': staff['name'],
                'role': staff['role'],
                'award_level': staff['award_level'],
                'status': 'active',
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow(),
            }

            self.cursor.execute("""
                INSERT INTO staff (
                    venue_id, name, role, award_level, status,
                    created_at, updated_at
                ) VALUES (
                    %(venue_id)s, %(name)s, %(role)s, %(award_level)s,
                    %(status)s, %(created_at)s, %(updated_at)s
                )
                RETURNING id
            """, staff_data)

            staff_id = self.cursor.fetchone()[0]
            staff_ids.append(staff_id)
            print(f"  Created: {staff['name']} ({staff['role']})")

        self.conn.commit()
        return staff_ids

    def seed_forecast(self, venue_id: int):
        """
        Generate 2 weeks of realistic pub demand forecast
        Weekday: lower demand
        Friday-Saturday: peak demand
        Patterns: lunch peak 12-2pm, dinner peak 6-9pm, late night peak Fri-Sat
        """
        print("\nGenerating 2 weeks of forecast data...")

        today = datetime.utcnow().date()
        forecasts = []

        for day_offset in range(14):
            current_date = today + timedelta(days=day_offset)
            weekday = current_date.weekday()  # 0=Monday, 6=Sunday

            # Demand curve factors by day of week
            if weekday in [4, 5]:  # Friday-Saturday (peak)
                base_demand = 0.8
            elif weekday == 6:  # Sunday (moderate)
                base_demand = 0.6
            else:  # Monday-Thursday
                base_demand = 0.4

            # Create hourly forecasts (10am-11pm opening hours)
            for hour in range(10, 23):
                # Natural demand curve throughout day
                if 10 <= hour < 12:  # Morning
                    demand = base_demand * 0.3
                elif 12 <= hour < 14:  # Lunch peak
                    demand = base_demand * 1.0
                elif 14 <= hour < 17:  # Afternoon slump
                    demand = base_demand * 0.3
                elif 17 <= hour < 20:  # Dinner prep/peak
                    demand = base_demand * 0.9
                elif 20 <= hour < 23:  # Evening
                    demand = base_demand * 0.6
                else:
                    demand = base_demand * 0.2

                # Add randomness (±20%)
                import random
                demand *= (0.8 + random.random() * 0.4)

                forecast_data = {
                    'venue_id': venue_id,
                    'forecast_date': current_date,
                    'hour': hour,
                    'predicted_covers': max(5, int(base_demand * 80 * (demand / base_demand))),
                    'confidence': min(0.95, 0.7 + random.random() * 0.25),
                    'model_type': 'prophet',
                    'created_at': datetime.utcnow(),
                }

                forecasts.append(forecast_data)

        # Batch insert all forecasts
        if forecasts:
            insert_query = """
                INSERT INTO forecasts (
                    venue_id, forecast_date, hour, predicted_covers,
                    confidence, model_type, created_at
                ) VALUES %s
            """
            values = [
                (
                    f['venue_id'],
                    f['forecast_date'],
                    f['hour'],
                    f['predicted_covers'],
                    f['confidence'],
                    f['model_type'],
                    f['created_at']
                )
                for f in forecasts
            ]
            execute_values(self.cursor, insert_query, values)
            self.conn.commit()

        print(f"Generated {len(forecasts)} forecast records")

    def seed_pos_data(self, venue_id: int):
        """
        Generate sample POS transaction data for the past 7 days
        Helps with demand pattern analysis
        """
        print("\nGenerating sample POS data...")

        today = datetime.utcnow().date()
        transactions = []

        for day_offset in range(7):
            current_date = today - timedelta(days=day_offset)
            weekday = current_date.weekday()

            # Determine revenue based on day type
            if weekday in [4, 5]:  # Friday-Saturday
                daily_revenue = 3000 + (int(current_date.timestamp()) % 1000)
                num_covers = 120 + (int(current_date.timestamp()) % 50)
            else:
                daily_revenue = 1200 + (int(current_date.timestamp()) % 800)
                num_covers = 50 + (int(current_date.timestamp()) % 40)

            transaction_data = {
                'venue_id': venue_id,
                'transaction_date': current_date,
                'total_revenue': daily_revenue,
                'covers': num_covers,
                'average_spend': daily_revenue / max(1, num_covers),
                'created_at': datetime.utcnow(),
            }

            transactions.append(transaction_data)

        # Batch insert
        if transactions:
            insert_query = """
                INSERT INTO pos_data (
                    venue_id, transaction_date, total_revenue, covers,
                    average_spend, created_at
                ) VALUES %s
            """
            values = [
                (
                    t['venue_id'],
                    t['transaction_date'],
                    t['total_revenue'],
                    t['covers'],
                    t['average_spend'],
                    t['created_at']
                )
                for t in transactions
            ]
            execute_values(self.cursor, insert_query, values)
            self.conn.commit()

        print(f"Generated {len(transactions)} POS records")

    def run(self):
        """Execute full seeding process"""
        try:
            print("\n" + "="*60)
            print("RosterIQ Demo Data Seeding")
            print("="*60)

            # Create venue
            venue_id = self.seed_venue()

            # Create user
            user_id, jwt_token = self.seed_user(venue_id)

            # Create staff
            staff_ids = self.seed_staff(venue_id)

            # Generate forecast
            self.seed_forecast(venue_id)

            # Generate POS data
            self.seed_pos_data(venue_id)

            # Print success summary
            print("\n" + "="*60)
            print("Demo Data Setup Complete!")
            print("="*60)
            print(f"\nVenue ID: {venue_id}")
            print(f"User ID: {user_id}")
            print(f"Staff Members: {len(staff_ids)}")
            print(f"\nDemo Account:")
            print(f"  Email: demo@rosteriq.com")
            print(f"  Password: demo123")
            print(f"\nJWT Token (valid for 30 days):")
            print(f"  {jwt_token}")
            print(f"\nYou can now:")
            print(f"  1. Log in to the dashboard at http://localhost:8000")
            print(f"  2. Use the JWT token in the Authorization header:")
            print(f"     Authorization: Bearer {jwt_token}")
            print(f"\n" + "="*60)

        except Exception as e:
            print(f"\nError during seeding: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
        finally:
            self.close()


if __name__ == '__main__':
    seeder = RosterIQSeeder()
    seeder.run()
