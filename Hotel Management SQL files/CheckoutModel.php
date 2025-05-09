<?php
require_once __DIR__ . '/../config/config.php';
require_once __DIR__ . '/../helpers/DatabaseHelper.php'; // Include DatabaseHelper

class CheckoutModel {
    public static function getAllCheckouts() {
        global $conn;
        $sql = "SELECT * FROM hotel_management.reservations WHERE status = 2"; // Status 2 for checked-out bookings
        $stmt = DatabaseHelper::executeQuery($conn, $sql, null);
        $result = mysqli_stmt_get_result($stmt);
        return mysqli_fetch_all($result, MYSQLI_ASSOC);
    }

    public static function checkoutBooking($reservationId) {
        global $conn;
        $sql = "UPDATE hotel_management.reservations SET status = 2 WHERE id = ?";
        $stmt = DatabaseHelper::executeQuery($conn, $sql, 'i', $reservationId);
        return $stmt ? true : false;
    }
}
?>
