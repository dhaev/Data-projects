<?php
require_once __DIR__ . '/../config/config.php';
require_once __DIR__ . '/../helpers/DatabaseHelper.php'; // Include DatabaseHelper

class CheckinModel {
    public static function getAllCheckins() {
        global $conn;
        $sql = "SELECT r.id AS BookID, 
                       CONCAT(c.fname, ' ', c.lname) AS CustomerName, 
                       r.start_date AS check_in 
                FROM reservations r 
                INNER JOIN customer c ON r.user_id = c.CustomerID 
                WHERE r.checked_in = 1 AND r.checked_out = 0";
        $stmt = DatabaseHelper::executeQuery($conn, $sql, null);
        $result = mysqli_stmt_get_result($stmt);
        return mysqli_fetch_all($result, MYSQLI_ASSOC);
    }

    public static function checkinBooking($reservationId) {
        global $conn;
        $sql = "UPDATE reservations SET checked_in = 1, date_checked_in = NOW() WHERE id = ?";
        $stmt = DatabaseHelper::executeQuery($conn, $sql, 'i', $reservationId);
        return $stmt ? true : false;
    }
}
?>
<!-- php
// Include database connection
require_once '../config.php';

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    // Get reservation ID from the request
    $reservation_id = isset($_POST['reservation_id']) ? intval($_POST['reservation_id']) : null;

    if (!$reservation_id) {
        http_response_code(400);
        echo json_encode(['error' => 'Reservation ID is required']);
        exit;
    }

    try {
        // Start database transaction
        $conn->begin_transaction();

        // Step 4: Create the temporary table
        $query1 = "CREATE TEMPORARY TABLE temp_get_taken_rooms (RoomID INT)";
        $conn->query($query1);

        // Step 4.1: Call the stored procedure and insert its results into the temporary table
        $query1_1 = "CALL GetTakenRooms(?)";
        $stmt1_1 = $conn->prepare($query1_1);
        $stmt1_1->bind_param('i', $reservation_id);
        $stmt1_1->execute();
        $result1_1 = $stmt1_1->get_result();

        while ($row = $result1_1->fetch_assoc()) {
            $query1_2 = "INSERT INTO temp_get_taken_rooms (RoomID) VALUES (?)";
            $stmt1_2 = $conn->prepare($query1_2);
            $stmt1_2->bind_param('i', $row['RoomID']);
            $stmt1_2->execute();
        }

        // Ensure the stored procedure's result set is fully processed
        while ($conn->more_results() && $conn->next_result()) {
            // Free any remaining results
        }

        // Step 5: Get type_id and num_rooms from reservation_details
        $query2 = "SELECT type_id, num_rooms FROM reservation_details WHERE reservation_id = ?";
        $stmt2 = $conn->prepare($query2);
        $stmt2->bind_param('i', $reservation_id);
        $stmt2->execute();
        $result2 = $stmt2->get_result();
        $types_and_rooms = $result2->fetch_all(MYSQLI_ASSOC);

        // Step 6: Prepare the insertion query
        $query4 = "INSERT INTO reservation_rooms (reservation_id, room_id) VALUES (?, ?)";
        $stmt4 = $conn->prepare($query4);

        // Step 7: Loop through type_id and num_rooms
        foreach ($types_and_rooms as $row) {
            $type_id = $row['type_id'];
            $num_rooms = $row['num_rooms'];

            // Fetch available rooms, excluding overlapping rooms
            $query3 = "
                SELECT RoomID 
                FROM room 
                WHERE RtypeID = ?
                  AND RoomID NOT IN (SELECT RoomID FROM temp_get_taken_rooms)
                LIMIT ?
            ";
            $stmt3 = $conn->prepare($query3);
            $stmt3->bind_param('ii', $type_id, $num_rooms);
            $stmt3->execute();
            $result3 = $stmt3->get_result();
            $available_rooms = $result3->fetch_all(MYSQLI_ASSOC);

            // Insert each available room into the reservations_rooms table
            foreach ($available_rooms as $room) {
                $stmt4->bind_param('ii', $reservation_id, $room['RoomID']);
                $stmt4->execute();
            }
        }

        // Update the reservations table to mark as checked in
        $query5 = "
            UPDATE reservations 
            SET checked_in = 1, date_checked_in = NOW(), status = 2 
            WHERE id = ?
        ";
        $stmt5 = $conn->prepare($query5);
        $stmt5->bind_param('i', $reservation_id);
        $stmt5->execute();

        // Commit transaction
        $conn->commit();
        http_response_code(200);
        echo json_encode(['success' => 'check out completed']);
    } catch (Exception $e) {
        // Rollback transaction on error
        $conn->rollback();
        http_response_code(500);
        echo json_encode(['error' => 'An error occurred', 'details' => $e->getMessage()]);
    }
} else {
    http_response_code(405);
    echo json_encode(['error' => 'Invalid request method']);
}
?> -->
