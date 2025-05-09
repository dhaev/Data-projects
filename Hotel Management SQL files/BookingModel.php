<?php
require_once __DIR__ . '/../config/config.php';
require_once __DIR__ . '/../config/secrets.php'; // Corrected path to secrets.php'; 
require_once __DIR__ . '/../helpers/DatabaseHelper.php'; // Include DatabaseHelper

class BookingModel {
    public static function getAllBookings() {
        global $conn;
        $sql = "SELECT r.id, c.fname, c.lname, r.start_date AS check_in, r.end_date AS check_out 
                FROM reservations r 
                INNER JOIN customer c ON r.user_id = c.CustomerID";
        $stmt = DatabaseHelper::executeQuery($conn, $sql, null);
        $result = mysqli_stmt_get_result($stmt);
        return mysqli_fetch_all($result, MYSQLI_ASSOC);
    }

    public static function getBookingById($id) {
        global $conn;

        $sql = "SELECT id, DATE(`start_date`) AS start_date, DATE(`end_date`) AS end_date, DATEDIFF(`end_date`, `start_date`) AS num_days 
                FROM `reservations` 
                WHERE id = ?";
        $stmt = DatabaseHelper::executeQuery($conn, $sql, 'i', $id);
        $result = mysqli_stmt_get_result($stmt);
        echo json_encode($result);
        return mysqli_fetch_assoc($result);
    }   

    public static function getBookingDetails($id) {
        global $conn;

        $sql = "WITH get_details AS (
                    SELECT id, type_id, num_rooms 
                    FROM `reservation_details` 
                    WHERE reservation_id = ?
                )
                SELECT id, type_id, num_rooms, price, (num_rooms * price) AS sub_total, rtype 
                FROM get_details 
                JOIN room_type ON type_id = RtypeID";
        $stmt = DatabaseHelper::executeQuery($conn, $sql, 'i', $id);
        $result = mysqli_stmt_get_result($stmt);
        echo json_encode($result);
        return mysqli_fetch_all($result, MYSQLI_ASSOC);
    }

    public static function addBooking($customerData, $checkin, $checkout, $rooms) {
        global $conn;
        $conn->begin_transaction();

        try {
            $stripe = new \Stripe\StripeClient(STRIPE_API_KEY);

            // Handle customer data
            if (isset($customerData['CustomerID'])) {
                $customerId = $customerData['CustomerID'];
            } else {
                $sql = "INSERT INTO customer (fname, lname, email, phone) VALUES (?, ?, ?, ?)";
                $stmt = DatabaseHelper::executeQuery($conn, $sql, 'ssss', $customerData['fname'], $customerData['lname'], $customerData['email'], $customerData['phone']);
                $customerId = mysqli_insert_id($conn);
            }

            // Insert booking details
            $sql = "INSERT INTO reservations (user_id, start_date, end_date) VALUES (?, ?, ?)";
            $stmt = DatabaseHelper::executeQuery($conn, $sql, 'iss', $customerId, $checkin, $checkout);
            $reservationId = mysqli_insert_id($conn);

            // Calculate total amount and prepare Stripe line items
            $totalAmount = 0;
            $currency = 'usd';
            $lineItems = [];
            $numDays = (new DateTime($checkin))->diff(new DateTime($checkout))->days;

            foreach ($rooms as $room) {
                $roomTypeId = $room['rtype'];
                $numRooms = $room['numr'];
                $roomTypeDetails = self::getRoomTypeDetails($conn, $roomTypeId);
                $pricePerRoom = $roomTypeDetails['price'];
                $roomTypeName = $roomTypeDetails['rtype'];

                $totalPrice = $pricePerRoom * $numRooms * $numDays;
                $totalAmount += $totalPrice;

                $lineItems[] = [
                    'price_data' => [
                        'currency' => $currency,
                        'product_data' => [
                            'name' => $roomTypeName,
                            'description' => "$numDays day(s): $checkin to $checkout",
                        ],
                        'unit_amount' => $pricePerRoom * 100,
                    ],
                    'quantity' => $numRooms,
                ];
            }

            // Create Stripe checkout session
            $metadata = [
                'user_id' => $customerId,
                'start_date' => $checkin,
                'end_date' => $checkout,
                'rooms' => json_encode($rooms),
            ];

            $checkoutSession = $stripe->checkout->sessions->create([
                'line_items' => $lineItems,
                'mode' => 'payment',
                'success_url' => BASE_URL . 'success.php',
                'cancel_url' => BASE_URL . 'cancel.php',
                'metadata' => $metadata,
            ]);

            // Commit transaction
            $conn->commit();

            // Return the Stripe checkout session URL
            return $checkoutSession->url;
        } catch (Exception $e) {
            $conn->rollback();
            throw $e;
        }
    }

    public static function editBooking($reservationId, $checkin, $checkout, $rooms) {
        global $conn;
        $conn->begin_transaction();

        try {
// Update booking dates
            $sql = "UPDATE hotel_management.reservations SET check_in = ?, check_out = ? WHERE id = ?";
            DatabaseHelper::executeQuery($conn, $sql, 'ssi', $checkin, $checkout, $reservationId);

            // Calculate the total amount previously charged
            $totalAmountPreviouslyCharged = self::calculateTotalAmount($conn, $reservationId);

            // Calculate the new total amount
            $totalAmountToCharge = 0;
            $numDays = (new DateTime($checkin))->diff(new DateTime($checkout))->days;

            foreach ($rooms as $room) {
                $roomTypeId = $room['rtype'];
                $numRooms = $room['numr'];
                $roomTypeDetails = self::getRoomTypeDetails($conn, $roomTypeId);
                $pricePerRoom = $roomTypeDetails['price'];
                $totalAmountToCharge += $pricePerRoom * $numRooms * $numDays;
            }

            // Determine if additional payment or refund is needed
            $amountDifference = $totalAmountToCharge - $totalAmountPreviouslyCharged;

            if ($amountDifference > 0) {
                // Additional payment required
                self::createPaymentIntent($amountDifference, $reservationId, $checkin, $checkout, $rooms);
            } elseif ($amountDifference < 0) {
                // Refund required
                $refundAmount = abs($amountDifference);
                self::processRefunds($conn, $reservationId, $refundAmount);
            }

            // Update reservation details


// Calculate the total amount previously charged
            $totalAmountPreviouslyCharged = self::calculateTotalAmount($conn, $reservationId);

            // Calculate the new total amount
            $totalAmountToCharge = 0;
            $numDays = (new DateTime($checkin))->diff(new DateTime($checkout))->days;

            foreach ($rooms as $room) {
                $roomTypeId = $room['rtype'];
                $numRooms = $room['numr'];
                $roomTypeDetails = self::getRoomTypeDetails($conn, $roomTypeId);
                $pricePerRoom = $roomTypeDetails['price'];
                $totalAmountToCharge += $pricePerRoom * $numRooms * $numDays;
            }

            // Determine if additional payment or refund is needed
            $amountDifference = $totalAmountToCharge - $totalAmountPreviouslyCharged;

            if ($amountDifference > 0) {
                // Additional payment required
                self::createPaymentIntent($amountDifference, $reservationId, $checkin, $checkout, $rooms);
            } elseif ($amountDifference < 0) {
                // Refund required
                $refundAmount = abs($amountDifference);
                self::processRefunds($conn, $reservationId, $refundAmount);
            }

            // Update reservation details
            $sqlDeleteDetails = "DELETE FROM reservation_details WHERE reservation_id = ?";
            DatabaseHelper::executeQuery($conn, $sqlDeleteDetails, 'i', $reservationId);

            foreach ($rooms as $room) {
                $roomTypeId = $room['rtype'];
                $numRooms = $room['numr'];
                $sqlDetails = "INSERT INTO reservation_details (reservation_id, type_id, num_rooms) VALUES (?, ?, ?)";
                DatabaseHelper::executeQuery($conn, $sqlDetails, 'iii', $reservationId, $roomTypeId, $numRooms);
            }

            $conn->commit();
            return true;
        } catch (Exception $e) {
            $conn->rollback();
            throw $e;
        }
    }

    public static function deleteBooking($id) {
        global $conn;
        $sql = "DELETE FROM reservations WHERE id = ?";
        $stmt = DatabaseHelper::executeQuery($conn, $sql, 'i', $id);
        return $stmt ;
    }    
    private static function calculateTotalAmount($conn, $reservationId) {
        $sql = "
        WITH RefundTotals AS (
            SELECT 
                payment_intent, 
                SUM(amount) AS total_refunds
            FROM 
                refunds
            GROUP BY 
                payment_intent
        )
        SELECT 
            SUM(COALESCE(payments.amount - RefundTotals.total_refunds, payments.amount)) AS total_amount
        FROM 
            payments
        LEFT JOIN 
            RefundTotals 
        ON 
            payments.payment_intent = RefundTotals.payment_intent
        WHERE 
            payments.reservation_id = ?
        GROUP BY 
            payments.reservation_id
        ";

        $stmt = DatabaseHelper::executeQuery($conn, $sql, 'i', $reservationId);
        $result = mysqli_stmt_get_result($stmt);
        $row = mysqli_fetch_assoc($result);
        return isset($row['total_amount']) ? $row['total_amount'] : 0;
    }

    private static function getRoomTypeDetails($conn, $roomTypeId) {
        $sql = "SELECT * FROM room_type WHERE RtypeID = ?";
        $stmt = DatabaseHelper::executeQuery($conn, $sql, 'i', $roomTypeId);
        $result = mysqli_stmt_get_result($stmt);
        return mysqli_fetch_assoc($result);
    }

    private static function createPaymentIntent($amount, $reservationId, $checkin, $checkout, $rooms) {
        $metadata = [
            'reservation_id' => $reservationId,
            'start_date' => $checkin,
            'end_date' => $checkout,
            'rooms' => json_encode($rooms)
        ];

        $paymentIntent = \Stripe\PaymentIntent::create([
            'amount' => $amount * 100, // Stripe expects the amount in cents
            'currency' => 'usd',
            'automatic_payment_methods' => ['enabled' => true],
            'metadata' => $metadata,
        ]);

        return $paymentIntent;
    }

    private static function processRefunds($conn, $reservationId, $refundAmount) {
        $refundableAmounts = self::fetchRefundableAmounts($conn, $reservationId);
        $remainingRefundAmount = $refundAmount * 100; // Convert to cents

        foreach ($refundableAmounts as $refundable) {
            if ($remainingRefundAmount <= 0) break;

            $refundAmountForIntent = min($remainingRefundAmount, $refundable['refundable_amount'] * 100);

            $refund = \Stripe\Refund::create([
                'payment_intent' => $refundable['payment_intent'],
                'amount' => $refundAmountForIntent,
            ]);

            if ($refund->status !== 'succeeded') {
                throw new Exception("Refund failed: " . $refund->status);
            }

            $sql = "INSERT INTO refunds (payment_id, payment_intent, amount, refund_date, stripe_refund_id) VALUES (?, ?, ?, NOW(), ?)";
            DatabaseHelper::executeQuery($conn, $sql, 'isds', $refundable['id'], $refundable['payment_intent'], $refundAmountForIntent / 100, $refund->id);

            $remainingRefundAmount -= $refundAmountForIntent;
        }

        if ($remainingRefundAmount > 0) {
            throw new Exception("Not enough funds to refund the full amount");
        }
    }

    private static function fetchRefundableAmounts($conn, $reservationId) {
        $sql = "
        WITH RefundTotals AS (
            SELECT 
                payment_intent, 
                SUM(amount) AS total_refunds
            FROM 
                refunds
            GROUP BY 
                payment_intent
        )
        SELECT 
            payments.id, 
            payments.payment_intent, 
            payments.amount, 
            RefundTotals.total_refunds, 
            COALESCE(payments.amount - RefundTotals.total_refunds, payments.amount) AS refundable_amount
        FROM 
            payments
        LEFT JOIN 
            RefundTotals 
        ON 
            payments.payment_intent = RefundTotals.payment_intent
        WHERE 
            payments.reservation_id = ?
        ORDER BY 
            payments.payment_date DESC
        ";

        $stmt = DatabaseHelper::executeQuery($conn, $sql, 'i', $reservationId);
        $result = mysqli_stmt_get_result($stmt);
        return mysqli_fetch_all($result, MYSQLI_ASSOC);
    }


}
?>