<?php
require_once __DIR__ . '/../config/config.php';
require_once __DIR__ . '/../helpers/DatabaseHelper.php'; // Include DatabaseHelper

class RoomModel {
    public static function getAllRooms() {
        global $conn;
        $sql = "SELECT r.RoomID, r.rnum, rt.rtype, rt.price, r.status 
                FROM room r 
                INNER JOIN room_type rt ON r.RtypeID = rt.RtypeID";
        $stmt = DatabaseHelper::executeQuery($conn, $sql, null);
        $result = mysqli_stmt_get_result($stmt);
        return mysqli_fetch_all($result, MYSQLI_ASSOC);
    }

    public static function getRoomById($id) {
        global $conn;
        $sql = "SELECT * FROM room WHERE RoomID = ?";
        $stmt = DatabaseHelper::executeQuery($conn, $sql, 'i', $id);
        $result = mysqli_stmt_get_result($stmt);
        return mysqli_fetch_assoc($result);
    }

    public static function addRoom($rnum, $RtypeID) {
        global $conn;
        $sql = "INSERT INTO room (rnum, RtypeID) VALUES (?, ?)";
        $stmt = DatabaseHelper::executeQuery($conn, $sql, 'ii', $rnum, $RtypeID);
        return $stmt ? true : false;
    }

    public static function editRoom($id, $rnum, $RtypeID, $status) {
        global $conn;
        $sql = "UPDATE room SET rnum = ?, RtypeID = ?, status = ? WHERE RoomID = ?";
        $stmt = DatabaseHelper::executeQuery($conn, $sql, 'iiii', $rnum, $RtypeID, $status, $id);
        return $stmt ? true : false;
    }

    public static function deleteRoom($id) {
        global $conn;
        $sql = "DELETE FROM room WHERE RoomID = ?";
        $stmt = DatabaseHelper::executeQuery($conn, $sql, 'i', $id);
        return $stmt ? true : false;
    }

    public static function getRoomAvailability($start, $end) {
        global $conn; // Use the global connection object

        if (!$start || !$end) {
            return ['error' => 'Invalid parameters'];
        }

        $sql = "CALL CheckRoomAvailability(?, ?)";
        $stmt =  $stmt = DatabaseHelper::executeQuery($conn, $sql, 'ss', $start, $end);
        $result = mysqli_stmt_get_result($stmt);
        return mysqli_fetch_assoc($result);
    }
}
?>
