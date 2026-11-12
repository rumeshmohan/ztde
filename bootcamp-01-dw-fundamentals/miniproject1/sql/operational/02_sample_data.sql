-- ============================================================
-- Sample Data for ABC Hub Operational Database
-- Used for testing the ETL pipeline
-- Run this in: abc_hub_operational
-- ============================================================

-- Countries
INSERT INTO country (country_id, country_name, country_code) VALUES
(1, 'United Kingdom', 'GB'),
(2, 'United States', 'US'),
(3, 'Australia', 'AU'),
(4, 'Canada', 'CA'),
(5, 'Sri Lanka', 'LK');

-- Cities
INSERT INTO city (city_id, country_id, city_name) VALUES
(1, 1, 'London'),
(2, 1, 'Manchester'),
(3, 2, 'New York'),
(4, 2, 'Los Angeles'),
(5, 3, 'Sydney'),
(6, 4, 'Toronto'),
(7, 5, 'Colombo');

-- Content Types
INSERT INTO content_type (content_type_id, content_type) VALUES
(1, 'Movie'),
(2, 'TV Series'),
(3, 'Documentary'),
(4, 'Music'),
(5, 'Book'),
(6, 'Video Game');

-- Genres
INSERT INTO genre (genre_id, genre_name) VALUES
(1, 'Action'),
(2, 'Comedy'),
(3, 'Drama'),
(4, 'Sci-Fi'),
(5, 'Horror'),
(6, 'Romance'),
(7, 'Thriller'),
(8, 'Documentary'),
(9, 'Animation'),
(10, 'Music');

-- Artists
INSERT INTO artist (artist_id, artist_name, country) VALUES
(1, 'Christopher Nolan', 'United Kingdom'),
(2, 'Emma Stone', 'United States'),
(3, 'David Attenborough', 'United Kingdom'),
(4, 'Taylor Swift', 'United States'),
(5, 'Hayao Miyazaki', 'Japan');

-- Devices
INSERT INTO device (device_id, device_name) VALUES
(1, 'Smart TV'),
(2, 'Mobile Phone'),
(3, 'Tablet'),
(4, 'Laptop'),
(5, 'Desktop Computer'),
(6, 'Gaming Console');

-- Subscription Plans
INSERT INTO subscription_plan (plan_id, plan_name, monthly_fee, video_quality, max_devices) VALUES
(1, 'Basic', 7.99, 'SD', 1),
(2, 'Standard', 12.99, 'HD', 2),
(3, 'Premium', 17.99, '4K', 4),
(4, 'Family', 22.99, '4K', 6);

-- Payment Methods
INSERT INTO payment_method (payment_method_id, method_name) VALUES
(1, 'Credit Card'),
(2, 'Debit Card'),
(3, 'PayPal'),
(4, 'Bank Transfer'),
(5, 'Gift Card');

-- Couriers
INSERT INTO courier (courier_id, courier_name) VALUES
(1, 'Royal Mail'),
(2, 'DHL Express'),
(3, 'FedEx'),
(4, 'UPS'),
(5, 'Local Courier');

-- Support Categories
INSERT INTO support_category (category_id, category_name) VALUES
(1, 'Billing'),
(2, 'Technical'),
(3, 'Content'),
(4, 'Delivery'),
(5, 'Account'),
(6, 'Refund');

-- Customers
INSERT INTO customer (customer_id, customer_no, first_name, last_name, email, phone, date_of_birth, gender, registration_date, status) VALUES
(1, 'CUST001', 'John', 'Smith', 'john.smith@email.com', '+44-7700-900001', '1990-05-15', 'Male', '2022-01-10', 'active'),
(2, 'CUST002', 'Sarah', 'Johnson', 'sarah.j@email.com', '+1-555-0101', '1985-08-22', 'Female', '2022-02-14', 'active'),
(3, 'CUST003', 'Mike', 'Williams', 'mike.w@email.com', '+44-7700-900003', '1992-11-30', 'Male', '2022-03-20', 'active'),
(4, 'CUST004', 'Emily', 'Brown', 'emily.b@email.com', '+61-4-0000-0001', '1988-03-08', 'Female', '2022-04-05', 'active'),
(5, 'CUST005', 'David', 'Lee', 'david.lee@email.com', '+1-555-0102', '1995-07-19', 'Male', '2022-05-12', 'inactive'),
(6, 'CUST006', 'Lisa', 'Davis', 'lisa.d@email.com', '+44-7700-900006', '1991-01-25', 'Female', '2022-06-18', 'active'),
(7, 'CUST007', 'James', 'Wilson', 'james.w@email.com', '+1-555-0103', '1987-09-14', 'Male', '2022-07-22', 'active'),
(8, 'CUST008', 'Anna', 'Taylor', 'anna.t@email.com', '+61-4-0000-0002', '1993-12-03', 'Female', '2022-08-30', 'active'),
(9, 'CUST009', 'Robert', 'Martinez', 'robert.m@email.com', '+1-555-0104', '1989-06-11', 'Male', '2023-01-15', 'active'),
(10, 'CUST010', 'Sophie', 'Anderson', 'sophie.a@email.com', '+44-7700-900010', '1996-04-28', 'Female', '2023-02-20', 'active');

-- Customer Addresses
INSERT INTO customer_address (address_id, customer_id, city_id, address_line, postal_code, address_type) VALUES
(1, 1, 1, '42 Baker Street', 'NW1 6XE', 'PRIMARY'),
(2, 2, 3, '123 Broadway Ave', '10001', 'PRIMARY'),
(3, 3, 2, '7 Oxford Road', 'M1 5QA', 'PRIMARY'),
(4, 4, 5, '15 George Street', '2000', 'PRIMARY'),
(5, 5, 4, '890 Sunset Blvd', '90028', 'PRIMARY'),
(6, 6, 1, '28 Kings Road', 'SW3 4RP', 'PRIMARY'),
(7, 7, 3, '456 Park Avenue', '10022', 'PRIMARY'),
(8, 8, 5, '22 Harbour Street', '2000', 'PRIMARY'),
(9, 9, 6, '99 Queen Street', 'M5V 2A5', 'PRIMARY'),
(10, 10, 1, '5 Chelsea Walk', 'SW10 0DL', 'PRIMARY');

-- Content
INSERT INTO content (content_id, content_type_id, title, release_date, duration_minutes, language, age_rating) VALUES
(1, 1, 'Inception', '2010-07-16', 148, 'English', 'PG-13'),
(2, 1, 'The Dark Knight', '2008-07-18', 152, 'English', 'PG-13'),
(3, 2, 'Breaking Bad', '2008-01-20', 47, 'English', '18'),
(4, 3, 'Planet Earth II', '2016-11-06', 50, 'English', 'PG'),
(5, 1, 'Spirited Away', '2001-07-20', 125, 'Japanese', 'PG'),
(6, 4, 'Midnights Album', '2022-10-21', 44, 'English', 'PG'),
(7, 5, 'Dune (Novel)', '1965-08-01', NULL, 'English', 'PG-13'),
(8, 6, 'The Last of Us', '2013-06-14', NULL, 'English', '18'),
(9, 1, 'Interstellar', '2014-11-07', 169, 'English', 'PG-13'),
(10, 2, 'Stranger Things', '2016-07-15', 51, 'English', '15'),
(11, 3, 'Our Planet', '2019-04-05', 50, 'English', 'PG'),
(12, 1, 'Parasite', '2019-05-30', 132, 'Korean', '15');

-- Content Genres
INSERT INTO content_genre (content_id, genre_id) VALUES
(1, 4), (1, 1), (2, 1), (2, 7), (3, 3), (3, 7),
(4, 8), (5, 9), (6, 10), (7, 4), (8, 1),
(9, 4), (9, 3), (10, 4), (10, 5), (11, 8), (12, 3), (12, 7);

-- Content Artists
INSERT INTO content_artist (content_id, artist_id, role) VALUES
(1, 1, 'Director'), (2, 1, 'Director'), (4, 3, 'Presenter'),
(5, 5, 'Director'), (6, 4, 'Artist'), (9, 1, 'Director'), (11, 3, 'Presenter');

-- Warehouses
INSERT INTO warehouse (warehouse_id, city_id, warehouse_name) VALUES
(1, 1, 'London Central Warehouse'),
(2, 3, 'New York Distribution Centre'),
(3, 5, 'Sydney Warehouse'),
(4, 6, 'Toronto Hub');

-- Inventory Items
INSERT INTO inventory_item (inventory_id, content_id, warehouse_id, barcode, purchase_date, item_condition, status) VALUES
(1, 1, 1, 'BC-001-INC-001', '2022-01-15', 'New', 'available'),
(2, 1, 1, 'BC-001-INC-002', '2022-01-15', 'Good', 'rented'),
(3, 2, 1, 'BC-002-DK-001', '2022-02-01', 'New', 'available'),
(4, 2, 2, 'BC-002-DK-002', '2022-02-01', 'Good', 'available'),
(5, 5, 3, 'BC-005-SA-001', '2022-03-10', 'New', 'rented'),
(6, 7, 1, 'BC-007-DN-001', '2022-04-01', 'New', 'available'),
(7, 7, 2, 'BC-007-DN-002', '2022-04-01', 'Good', 'available'),
(8, 8, 2, 'BC-008-LU-001', '2022-05-15', 'New', 'rented'),
(9, 8, 4, 'BC-008-LU-002', '2022-05-15', 'New', 'available'),
(10, 12, 1, 'BC-012-PA-001', '2022-06-01', 'New', 'available');

-- Customer Subscriptions
INSERT INTO customer_subscription (subscription_id, customer_id, plan_id, start_date, end_date, status, auto_renew) VALUES
(1, 1, 3, '2022-01-10', NULL, 'active', TRUE),
(2, 2, 2, '2022-02-14', NULL, 'active', TRUE),
(3, 3, 3, '2022-03-20', NULL, 'active', TRUE),
(4, 4, 2, '2022-04-05', NULL, 'active', TRUE),
(5, 5, 1, '2022-05-12', '2023-01-12', 'cancelled', FALSE),
(6, 6, 4, '2022-06-18', NULL, 'active', TRUE),
(7, 7, 2, '2022-07-22', NULL, 'active', TRUE),
(8, 8, 3, '2022-08-30', NULL, 'active', TRUE),
(9, 9, 2, '2023-01-15', NULL, 'active', TRUE),
(10, 10, 1, '2023-02-20', NULL, 'active', TRUE);

-- Streaming Sessions
INSERT INTO streaming_session (stream_id, customer_id, content_id, device_id, start_time, end_time, watch_duration, completion_percentage) VALUES
(1, 1, 1, 1, '2024-01-15 19:00:00', '2024-01-15 21:28:00', 148, 100.00),
(2, 1, 9, 1, '2024-01-16 20:00:00', '2024-01-16 22:49:00', 169, 100.00),
(3, 2, 3, 2, '2024-01-15 12:00:00', '2024-01-15 12:47:00', 47, 100.00),
(4, 2, 3, 2, '2024-01-15 13:00:00', '2024-01-15 13:45:00', 45, 95.74),
(5, 3, 10, 4, '2024-01-16 21:00:00', '2024-01-16 21:51:00', 51, 100.00),
(6, 4, 4, 1, '2024-01-17 18:00:00', '2024-01-17 18:50:00', 50, 100.00),
(7, 6, 1, 3, '2024-01-17 14:00:00', '2024-01-17 15:30:00', 90, 60.81),
(8, 7, 12, 1, '2024-01-18 20:00:00', '2024-01-18 22:12:00', 132, 100.00),
(9, 8, 5, 2, '2024-01-18 09:00:00', '2024-01-18 11:05:00', 125, 100.00),
(10, 9, 11, 4, '2024-01-19 19:30:00', '2024-01-19 20:20:00', 50, 100.00),
(11, 10, 10, 2, '2024-01-19 22:00:00', '2024-01-19 22:30:00', 30, 58.82),
(12, 1, 4, 1, '2024-01-20 10:00:00', '2024-01-20 10:50:00', 50, 100.00),
(13, 3, 1, 6, '2024-01-20 15:00:00', '2024-01-20 17:28:00', 148, 100.00),
(14, 6, 9, 1, '2024-01-20 20:00:00', '2024-01-20 22:49:00', 169, 100.00),
(15, 2, 10, 4, '2024-01-21 11:00:00', '2024-01-21 11:51:00', 51, 100.00);

-- Rentals
INSERT INTO rental (rental_id, customer_id, inventory_id, rental_date, due_date, return_date, rental_fee, late_fee, status) VALUES
(1, 1, 6, '2024-01-15', '2024-01-22', '2024-01-20', 4.99, 0.00, 'returned'),
(2, 2, 3, '2024-01-16', '2024-01-23', '2024-01-25', 4.99, 2.00, 'returned'),
(3, 3, 8, '2024-01-17', '2024-01-24', NULL, 5.99, 0.00, 'active'),
(4, 4, 5, '2024-01-18', '2024-01-25', '2024-01-24', 4.99, 0.00, 'returned'),
(5, 7, 10, '2024-01-19', '2024-01-26', NULL, 4.99, 0.00, 'active'),
(6, 1, 4, '2024-01-20', '2024-01-27', '2024-01-26', 4.99, 0.00, 'returned'),
(7, 9, 7, '2024-01-20', '2024-01-27', NULL, 4.99, 0.00, 'active'),
(8, 6, 1, '2024-01-21', '2024-01-28', NULL, 4.99, 0.00, 'active');

-- Payments
INSERT INTO payment (payment_id, customer_id, rental_id, subscription_id, payment_method_id, amount, payment_date, payment_type, status) VALUES
(1, 1, NULL, 1, 1, 17.99, '2024-01-10 09:00:00', 'subscription', 'completed'),
(2, 2, NULL, 2, 3, 12.99, '2024-01-14 10:00:00', 'subscription', 'completed'),
(3, 1, 1, NULL, 1, 4.99, '2024-01-15 14:00:00', 'rental', 'completed'),
(4, 2, 2, NULL, 3, 6.99, '2024-01-16 11:00:00', 'rental', 'completed'),
(5, 3, 3, NULL, 2, 5.99, '2024-01-17 16:00:00', 'rental', 'completed'),
(6, 4, 4, NULL, 1, 4.99, '2024-01-18 13:00:00', 'rental', 'completed'),
(7, 3, NULL, 3, 2, 17.99, '2024-01-20 09:00:00', 'subscription', 'completed'),
(8, 7, 5, NULL, 4, 4.99, '2024-01-19 15:00:00', 'rental', 'completed'),
(9, 1, 6, NULL, 1, 4.99, '2024-01-20 10:00:00', 'rental', 'completed'),
(10, 6, NULL, 6, 1, 22.99, '2024-01-18 09:00:00', 'subscription', 'completed'),
(11, 9, 7, NULL, 3, 4.99, '2024-01-20 14:00:00', 'rental', 'completed'),
(12, 6, 8, NULL, 1, 4.99, '2024-01-21 12:00:00', 'rental', 'completed');

-- Deliveries
INSERT INTO delivery (delivery_id, rental_id, courier_id, dispatch_date, delivered_date, returned_date, delivery_status) VALUES
(1, 1, 1, '2024-01-15 15:00:00', '2024-01-16 10:00:00', '2024-01-20 14:00:00', 'completed'),
(2, 2, 3, '2024-01-16 12:00:00', '2024-01-17 09:00:00', '2024-01-25 11:00:00', 'completed'),
(3, 3, 3, '2024-01-17 17:00:00', '2024-01-18 11:00:00', NULL, 'delivered'),
(4, 4, 2, '2024-01-18 14:00:00', '2024-01-19 08:00:00', '2024-01-24 10:00:00', 'completed'),
(5, 5, 3, '2024-01-19 16:00:00', '2024-01-20 10:00:00', NULL, 'delivered'),
(6, 6, 4, '2024-01-20 11:00:00', '2024-01-21 09:00:00', '2024-01-26 15:00:00', 'completed'),
(7, 7, 4, '2024-01-20 15:00:00', '2024-01-21 11:00:00', NULL, 'delivered'),
(8, 8, 1, '2024-01-21 13:00:00', '2024-01-22 09:00:00', NULL, 'delivered');

-- Reviews
INSERT INTO review (review_id, customer_id, content_id, rating, review_text, review_date) VALUES
(1, 1, 1, 5, 'Absolutely brilliant film, Nolan at his best.', '2024-01-16 08:00:00'),
(2, 2, 3, 5, 'One of the greatest TV series ever made.', '2024-01-16 14:00:00'),
(3, 3, 10, 4, 'Great show, perfect for binge watching.', '2024-01-17 09:00:00'),
(4, 4, 4, 5, 'Stunning visuals, incredible documentary.', '2024-01-18 10:00:00'),
(5, 7, 12, 5, 'Masterpiece of Korean cinema.', '2024-01-19 08:00:00'),
(6, 8, 5, 5, 'Beautiful animation and storytelling.', '2024-01-19 12:00:00'),
(7, 1, 9, 4, 'Visually stunning, emotionally powerful.', '2024-01-17 09:00:00'),
(8, 6, 1, 4, 'Great concept, complex plot.', '2024-01-18 15:00:00');

-- Wishlists
INSERT INTO wishlist (wishlist_id, customer_id, content_id, added_date) VALUES
(1, 1, 12, '2024-01-14 20:00:00'),
(2, 2, 5, '2024-01-15 18:00:00'),
(3, 3, 9, '2024-01-16 12:00:00'),
(4, 5, 1, '2024-01-17 14:00:00'),
(5, 10, 4, '2024-01-18 16:00:00'),
(6, 9, 12, '2024-01-19 20:00:00'),
(7, 6, 11, '2024-01-20 10:00:00'),
(8, 7, 5, '2024-01-21 08:00:00');

-- Recommendations
INSERT INTO recommendation (recommendation_id, customer_id, content_id, recommendation_date, algorithm_version, clicked) VALUES
(1, 1, 12, '2024-01-15 10:00:00', 'v2.1', TRUE),
(2, 2, 5, '2024-01-15 10:00:00', 'v2.1', TRUE),
(3, 3, 9, '2024-01-16 10:00:00', 'v2.1', FALSE),
(4, 4, 11, '2024-01-17 10:00:00', 'v2.1', TRUE),
(5, 6, 4, '2024-01-18 10:00:00', 'v2.1', FALSE),
(6, 7, 1, '2024-01-19 10:00:00', 'v2.1', TRUE),
(7, 8, 12, '2024-01-19 10:00:00', 'v2.1', FALSE),
(8, 9, 10, '2024-01-20 10:00:00', 'v2.1', TRUE);

-- Support Tickets
INSERT INTO support_ticket (ticket_id, customer_id, category_id, opened_date, closed_date, priority, status) VALUES
(1, 2, 2, '2024-01-16 08:00:00', '2024-01-16 12:00:00', 'medium', 'resolved'),
(2, 5, 1, '2024-01-17 10:00:00', '2024-01-18 09:00:00', 'high', 'resolved'),
(3, 3, 4, '2024-01-18 14:00:00', NULL, 'low', 'open'),
(4, 1, 3, '2024-01-19 11:00:00', '2024-01-19 15:00:00', 'low', 'resolved'),
(5, 7, 2, '2024-01-20 09:00:00', NULL, 'medium', 'open'),
(6, 10, 5, '2024-01-21 16:00:00', NULL, 'low', 'open');
