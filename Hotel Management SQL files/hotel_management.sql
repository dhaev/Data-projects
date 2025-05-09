-- phpMyAdmin SQL Dump
-- version 5.2.1
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1:3306
-- Generation Time: Mar 27, 2025 at 05:29 PM
-- Server version: 9.1.0
-- PHP Version: 8.3.14

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `hotel_management`
--

DELIMITER $$
--
-- Procedures
--
DROP PROCEDURE IF EXISTS `CheckRoomAvailability`$$
CREATE DEFINER=`root`@`localhost` PROCEDURE `CheckRoomAvailability` (IN `start_date` DATE, IN `end_date` DATE)   BEGIN
  -- Declare a variable to check if there are overlapping reservations
  DECLARE reservation_count INT;

  -- Create a temporary table to store filtered reservations
  CREATE TEMPORARY TABLE temp_check_reservations AS
    SELECT id
    FROM reservations
    WHERE (status != 0 OR status != 1)
      AND (
        (reservations.start_date < end_date AND reservations.end_date > start_date)
        OR (reservations.start_date < start_date AND reservations.end_date > end_date)
        OR (start_date BETWEEN reservations.start_date AND reservations.end_date)
        OR (end_date BETWEEN reservations.start_date AND reservations.end_date)
        OR (reservations.start_date = start_date)
        OR (reservations.end_date = end_date)
      );

  -- Count overlapping reservations
  SELECT COUNT(*) INTO reservation_count
  FROM temp_check_reservations;

  -- Conditional logic based on reservation_count
  IF reservation_count = 0 THEN
    -- No overlapping reservations; return all room summaries
    SELECT no_rooms AS no_of_available_rooms, RtypeID, price, rtype FROM view_room_summary;
  ELSE
    -- Overlapping reservations exist; calculate available rooms
    SELECT
	CAST(COALESCE(view_room_summary.no_rooms - SUM(num_rooms), view_room_summary.no_rooms) AS UNSIGNED) AS no_of_available_rooms, view_room_summary.RtypeID, price, rtype
    FROM reservation_details
    JOIN temp_check_reservations 
      ON reservation_details.reservation_id = temp_check_reservations.id
     RIGHT JOIN view_room_summary 
     ON type_id = view_room_summary.RtypeID
     GROUP BY type_id;
  END IF;

  -- Drop the temporary table
  DROP TEMPORARY TABLE temp_check_reservations;
END$$

DROP PROCEDURE IF EXISTS `GetTakenRooms`$$
CREATE DEFINER=`root`@`localhost` PROCEDURE `GetTakenRooms` (IN `reservation_id_variable` INT)   BEGIN
    -- Declare variables to hold start and end dates
    DECLARE param_start_date DATE;
    DECLARE param_end_date DATE;

    -- Step 1: Fetch start and end dates for the given reservation ID
    SELECT start_date, end_date 
    INTO param_start_date, param_end_date
    FROM reservations 
    WHERE id = reservation_id_variable;

    -- Step 2: Use a Common Table Expression (CTE) to find overlapping reservations
    WITH temp_check_reservations AS (
        SELECT id
        FROM reservations
        WHERE (status != 0 OR status != 1)
          AND (
               (reservations.start_date < param_end_date AND reservations.end_date > param_start_date)
               OR (reservations.start_date < param_start_date AND reservations.end_date > param_end_date)
               OR (param_start_date BETWEEN reservations.start_date AND reservations.end_date)
               OR (param_end_date BETWEEN reservations.start_date AND reservations.end_date)
               OR (reservations.start_date = param_start_date)
               OR (reservations.end_date = param_end_date)
             )
    )
    -- Step 3: Fetch taken rooms by joining overlapping reservations with reservation_rooms
    SELECT reservation_rooms.id , reservation_rooms.reservation_id, reservation_rooms.room_id
    FROM temp_check_reservations
    JOIN reservation_rooms 
    ON temp_check_reservations.id = reservation_rooms.reservation_id;
END$$

DELIMITER ;

-- --------------------------------------------------------

--
-- Table structure for table `customer`
--

DROP TABLE IF EXISTS `customer`;
CREATE TABLE IF NOT EXISTS `customer` (
  `CustomerID` int NOT NULL AUTO_INCREMENT,
  `fname` varchar(255) NOT NULL,
  `lname` varchar(255) NOT NULL,
  `email` varchar(255) NOT NULL,
  `phone` varchar(255) NOT NULL,
  `address` varchar(255) DEFAULT NULL,
  `country` varchar(255) DEFAULT NULL,
  `city` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`CustomerID`)
) ENGINE=MyISAM AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Dumping data for table `customer`
--

INSERT INTO `customer` (`CustomerID`, `fname`, `lname`, `email`, `phone`, `address`, `country`, `city`) VALUES
(1, 'Domp', 'Abodunrin', 'davidabodunrin735@gmail.com', '7093155889', '', '', ''),
(2, 'David', 'Abodunrin', 'vicabod65@gmail.com', '7093155889', NULL, NULL, NULL),
(3, 'David', 'Abodunrin', 'gjhg@me.com', '7093155889', NULL, NULL, NULL),
(4, 'David', 'Abodunrin', 'me@gmail.cm', '7093155889', NULL, NULL, NULL),
(5, '<serrs>', 'grgrgg', 'grgrgglogo@bron.coms', '9876556789', NULL, NULL, NULL);

-- --------------------------------------------------------

--
-- Table structure for table `employees`
--

DROP TABLE IF EXISTS `employees`;
CREATE TABLE IF NOT EXISTS `employees` (
  `CustomerID` int NOT NULL AUTO_INCREMENT,
  `fname` varchar(255) NOT NULL,
  `lname` varchar(255) NOT NULL,
  `email` varchar(255) NOT NULL,
  `phone` varchar(255) NOT NULL,
  `address` varchar(255) DEFAULT NULL,
  `country` varchar(255) DEFAULT NULL,
  `city` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`CustomerID`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `maintenance`
--

DROP TABLE IF EXISTS `maintenance`;
CREATE TABLE IF NOT EXISTS `maintenance` (
  `RoomID` int DEFAULT NULL,
  `status` tinyint NOT NULL,
  KEY `RoomID` (`RoomID`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Dumping data for table `maintenance`
--

INSERT INTO `maintenance` (`RoomID`, `status`) VALUES
(1, 1),
(1, 1),
(1, 1),
(12, 1);

-- --------------------------------------------------------

--
-- Table structure for table `payments`
--

DROP TABLE IF EXISTS `payments`;
CREATE TABLE IF NOT EXISTS `payments` (
  `id` int NOT NULL AUTO_INCREMENT,
  `reservation_id` int NOT NULL,
  `amount` decimal(10,2) NOT NULL,
  `currency` varchar(10) NOT NULL,
  `payment_status` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `payment_date` datetime NOT NULL,
  `stripe_payment_id` varchar(255) NOT NULL,
  `payment_intent` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `reservation_id` (`reservation_id`),
  KEY `idx_payment_intent` (`payment_intent`(250))
) ENGINE=MyISAM AUTO_INCREMENT=24 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Dumping data for table `payments`
--

INSERT INTO `payments` (`id`, `reservation_id`, `amount`, `currency`, `payment_status`, `payment_date`, `stripe_payment_id`, `payment_intent`) VALUES
(23, 13, 175.00, 'usd', 'paid', '2025-03-24 10:05:39', 'cs_test_a15yZ9uGYWAc2baD8fzMKgJG2EXDpnkboasgk7y1S9NxQFoqfrq0fsFZvz', 'pi_3R6AhARq0GzSOwDw1EYHMVay');

-- --------------------------------------------------------

--
-- Table structure for table `refunds`
--

DROP TABLE IF EXISTS `refunds`;
CREATE TABLE IF NOT EXISTS `refunds` (
  `id` int NOT NULL AUTO_INCREMENT,
  `payment_id` int NOT NULL,
  `payment_intent` varchar(255) NOT NULL,
  `amount` decimal(10,2) NOT NULL,
  `refund_date` datetime NOT NULL,
  `stripe_refund_id` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `payment_id` (`payment_id`),
  KEY `rfd_pm_payment_index` (`payment_intent`(250))
) ENGINE=MyISAM AUTO_INCREMENT=18 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `registered`
--

DROP TABLE IF EXISTS `registered`;
CREATE TABLE IF NOT EXISTS `registered` (
  `CustomerID` int DEFAULT NULL,
  `email` varchar(50) DEFAULT NULL,
  `password` varchar(255) NOT NULL,
  `image` varchar(255) DEFAULT NULL,
  `created_at` datetime NOT NULL,
  UNIQUE KEY `email` (`email`),
  KEY `CustomerID` (`CustomerID`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Dumping data for table `registered`
--

INSERT INTO `registered` (`CustomerID`, `email`, `password`, `image`, `created_at`) VALUES
(1, 'davidabodunrin735@gmail.com', '$2y$10$klVELt/cuPrMaFqxWMqJMO2EupbLqrQRJcHrQVUZC9ElNlzqg5ft2', 'profile1_67d9926ee51042.34853561.jpg', '0000-00-00 00:00:00'),
(5, 'grgrgglogo@bron.coms', '$2y$10$IYHg7.VboLvsa83oCvWShOtYeTlGCFJf4FrPz6zU9oTGlZZd0yXRO', 'default.png', '0000-00-00 00:00:00');

-- --------------------------------------------------------

--
-- Table structure for table `reservations`
--

DROP TABLE IF EXISTS `reservations`;
CREATE TABLE IF NOT EXISTS `reservations` (
  `id` int NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `start_date` date NOT NULL,
  `end_date` date NOT NULL,
  `checked_in` tinyint(1) NOT NULL DEFAULT '0',
  `date_checked_in` datetime DEFAULT NULL,
  `checked_out` tinyint(1) NOT NULL DEFAULT '0',
  `date_checked_out` datetime DEFAULT NULL,
  `cancelled` tinyint(1) NOT NULL DEFAULT '0',
  `date_cancelled` datetime DEFAULT NULL,
  `status` tinyint NOT NULL DEFAULT '5',
  PRIMARY KEY (`id`),
  KEY `user_id` (`user_id`),
  KEY `fk_status_code` (`status`)
) ENGINE=MyISAM AUTO_INCREMENT=14 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Dumping data for table `reservations`
--

INSERT INTO `reservations` (`id`, `user_id`, `start_date`, `end_date`, `checked_in`, `date_checked_in`, `checked_out`, `date_checked_out`, `cancelled`, `date_cancelled`, `status`) VALUES
(13, 1, '2025-03-24', '2025-03-25', 1, '2025-03-25 22:00:39', 0, NULL, 0, NULL, 2);

-- --------------------------------------------------------

--
-- Table structure for table `reservation_details`
--

DROP TABLE IF EXISTS `reservation_details`;
CREATE TABLE IF NOT EXISTS `reservation_details` (
  `id` int NOT NULL AUTO_INCREMENT,
  `reservation_id` int NOT NULL,
  `type_id` int NOT NULL,
  `num_rooms` int NOT NULL,
  PRIMARY KEY (`id`),
  KEY `reservation_id` (`reservation_id`),
  KEY `type_id` (`type_id`)
) ENGINE=MyISAM AUTO_INCREMENT=86 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Dumping data for table `reservation_details`
--

INSERT INTO `reservation_details` (`id`, `reservation_id`, `type_id`, `num_rooms`) VALUES
(85, 13, 2, 1);

-- --------------------------------------------------------

--
-- Table structure for table `reservation_rooms`
--

DROP TABLE IF EXISTS `reservation_rooms`;
CREATE TABLE IF NOT EXISTS `reservation_rooms` (
  `id` int NOT NULL AUTO_INCREMENT,
  `reservation_id` int NOT NULL,
  `room_id` int NOT NULL,
  PRIMARY KEY (`id`),
  KEY `reservation_id` (`reservation_id`),
  KEY `room_id` (`room_id`)
) ENGINE=MyISAM AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Dumping data for table `reservation_rooms`
--

INSERT INTO `reservation_rooms` (`id`, `reservation_id`, `room_id`) VALUES
(1, 13, 13);

-- --------------------------------------------------------

--
-- Table structure for table `reservation_status_code`
--

DROP TABLE IF EXISTS `reservation_status_code`;
CREATE TABLE IF NOT EXISTS `reservation_status_code` (
  `code` tinyint NOT NULL,
  `description` varchar(50) NOT NULL,
  PRIMARY KEY (`code`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Dumping data for table `reservation_status_code`
--

INSERT INTO `reservation_status_code` (`code`, `description`) VALUES
(0, 'cancelled'),
(1, 'checkedOut'),
(2, 'checkedIn'),
(3, 'reserved'),
(4, 'pending');

-- --------------------------------------------------------

--
-- Table structure for table `room`
--

DROP TABLE IF EXISTS `room`;
CREATE TABLE IF NOT EXISTS `room` (
  `RoomID` int NOT NULL AUTO_INCREMENT,
  `rnum` int NOT NULL,
  `RtypeID` int NOT NULL,
  `status` tinyint DEFAULT '0',
  PRIMARY KEY (`RoomID`),
  KEY `RtypeID` (`RtypeID`)
) ENGINE=MyISAM AUTO_INCREMENT=15 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Dumping data for table `room`
--

INSERT INTO `room` (`RoomID`, `rnum`, `RtypeID`, `status`) VALUES
(1, 101, 1, 0),
(12, 123, 1, 0),
(13, 201, 2, 0),
(14, 202, 2, 0);

-- --------------------------------------------------------

--
-- Table structure for table `rooms`
--

DROP TABLE IF EXISTS `rooms`;
CREATE TABLE IF NOT EXISTS `rooms` (
  `id` int NOT NULL AUTO_INCREMENT,
  `room_number` varchar(10) NOT NULL,
  `type_id` int NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `room_number` (`room_number`),
  KEY `type_id` (`type_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `room_type`
--

DROP TABLE IF EXISTS `room_type`;
CREATE TABLE IF NOT EXISTS `room_type` (
  `RtypeID` int NOT NULL AUTO_INCREMENT,
  `rtype` varchar(255) NOT NULL,
  `price` decimal(10,2) NOT NULL,
  `description` text,
  `image` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`RtypeID`)
) ENGINE=MyISAM AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Dumping data for table `room_type`
--

INSERT INTO `room_type` (`RtypeID`, `rtype`, `price`, `description`, `image`) VALUES
(1, 'Single', 175.00, ' kjb', 'Single_67d6d757759112.73990031.jpg'),
(2, 'ingle', 175.00, ' kjb', 'ingle_67d6d9d1084395.01704983.jpg'),
(3, 'twin', 789.00, ' lkhg', 'twin_67d6da01653db7.98162427.jpg'),
(4, 'Noble', 186.00, ' noble numbat', 'Noble_67d861c21f6705.08662424.jpg'),
(5, 'nougat', 200.00, ' nougattt', 'nougat_67d97bb7c76329.55706722.jpg');

-- --------------------------------------------------------

--
-- Table structure for table `room_types`
--

DROP TABLE IF EXISTS `room_types`;
CREATE TABLE IF NOT EXISTS `room_types` (
  `id` int NOT NULL AUTO_INCREMENT,
  `type_name` varchar(50) NOT NULL,
  `price` decimal(10,2) NOT NULL,
  `description` text,
  `image` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Stand-in structure for view `view_room_summary`
-- (See below for the actual view)
--
DROP VIEW IF EXISTS `view_room_summary`;
CREATE TABLE IF NOT EXISTS `view_room_summary` (
`no_rooms` bigint
,`price` decimal(10,2)
,`rtype` varchar(255)
,`RtypeID` int
);

-- --------------------------------------------------------

--
-- Structure for view `view_room_summary`
--
DROP TABLE IF EXISTS `view_room_summary`;

DROP VIEW IF EXISTS `view_room_summary`;
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `view_room_summary`  AS SELECT count(`room`.`RoomID`) AS `no_rooms`, `room`.`RtypeID` AS `RtypeID`, `room_type`.`rtype` AS `rtype`, `room_type`.`price` AS `price` FROM (`room` join `room_type` on((`room`.`RtypeID` = `room_type`.`RtypeID`))) GROUP BY `room`.`RtypeID`, `room_type`.`rtype`, `room_type`.`price` ;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
