<?php
require_once __DIR__ . '/../config/config.php';
require_once __DIR__ . '/../secrets.php';
require_once __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/../helpers/DatabaseHelper.php'; // Include DatabaseHelper

class CancelModel {
    public static function getAllCancellations() {
        global $conn;
        $sql = "SELECT * FROM hotel_management.reservations WHERE status = 3"; // Status 3 for canceled bookings
        $stmt = DatabaseHelper::executeQuery($conn, $sql, null);
        $result = mysqli_stmt_get_result($stmt);
        return mysqli_fetch_all($result, MYSQLI_ASSOC);
    }

    public static function cancelBooking($reservationId) {
        global $conn;
        $sql = "UPDATE hotel_management.reservations SET status = 3 WHERE id = ?";
        $stmt = DatabaseHelper::executeQuery($conn, $sql, 'i', $reservationId);
        return $stmt ? true : false;
    }

    public static function cancelReservation($reservationId) {
        global $conn;
        \Stripe\Stripe::setApiKey(STRIPE_API_KEY);

        $conn->begin_transaction();

        try {
            // Fetch payment details
            $sql = "SELECT id, stripe_payment_id, amount FROM payments WHERE reservation_id = ?";
            $stmt = DatabaseHelper::executeQuery($conn, $sql, 'i', $reservationId);
            $result = mysqli_stmt_get_result($stmt);
            $payment = mysqli_fetch_assoc($result);
            mysqli_stmt_close($stmt);

            if (!$payment) {
                throw new Exception("Payment details not found for reservation ID: $reservationId");
            }

            $paymentId = $payment['id'];
            $stripePaymentId = $payment['stripe_payment_id'];
            $amount = $payment['amount'];

            // Process refund with Stripe
            $refund = \Stripe\Refund::create([
                'payment_intent' => $stripePaymentId,
                'amount' => $amount * 100, // Stripe expects the amount in cents
            ]);

            // Store refund details
            $sql = "INSERT INTO refunds (payment_id, amount, refund_date, stripe_refund_id) VALUES (?, ?, NOW(), ?)";
            DatabaseHelper::executeQuery($conn, $sql, 'ids', $paymentId, $amount, $refund->id);

            // Delete reservation details
            $sql = "DELETE FROM reservation_details WHERE reservation_id = ?";
            DatabaseHelper::executeQuery($conn, $sql, 'i', $reservationId);

            // Delete reservation rooms
            $sql = "DELETE FROM reservation_rooms WHERE reservation_id = ?";
            DatabaseHelper::executeQuery($conn, $sql, 'i', $reservationId);

            // Delete reservation
            $sql = "DELETE FROM reservations WHERE id = ?";
            DatabaseHelper::executeQuery($conn, $sql, 'i', $reservationId);

            // Commit transaction
            $conn->commit();
            return ['status' => 'success', 'message' => 'Reservation cancelled and refund processed successfully!'];
        } catch (Exception $e) {
            // Rollback transaction on error
            $conn->rollback();
            return ['status' => 'error', 'message' => $e->getMessage()];
        }
    }
}
?>
