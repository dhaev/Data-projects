<?php
require_once __DIR__ . '/../config/config.php';
require_once __DIR__ . '/../helpers/DatabaseHelper.php'; // Include DatabaseHelper

class RoomTypeModel {
    public static function getAllRoomTypes() {
        global $conn;
        $sql = "SELECT * FROM room_type";
        $stmt = DatabaseHelper::executeQuery($conn, $sql, null);
        $result = mysqli_stmt_get_result($stmt);
        return mysqli_fetch_all($result, MYSQLI_ASSOC);
    }

    public static function getRoomTypeById($id) {
        global $conn;
        $sql = "SELECT * FROM room_type WHERE RtypeID = ?";
        $stmt = DatabaseHelper::executeQuery($conn, $sql, 'i', $id);
        $result = mysqli_stmt_get_result($stmt);
        return mysqli_fetch_assoc($result);
    }

    public static function addRoomType($rtype, $price, $description, $imagePath) {
        global $conn;

        $sql = "INSERT INTO room_type (rtype, price, description, image) VALUES (?, ?, ?, ?)";
        $stmt = DatabaseHelper::executeQuery($conn, $sql, 'sdss', $rtype, $price, $description, $imagePath);
        return $stmt ? true : false;
    }

    public static function editRoomType($id, $rtype, $price, $description, $imagePath) {
        global $conn;
        $sql = "UPDATE room_type SET rtype = ?, price = ?, description = ?, image = ? WHERE RtypeID = ?";
        $stmt = DatabaseHelper::executeQuery($conn, $sql, 'sdssi', $rtype, $price, $description, $imagePath, $id);
        return $stmt ? true : false;
    }

    public static function deleteRoomType($id) {
        global $conn;
        $sql = "DELETE FROM room_type WHERE RtypeID = ?";
        $stmt = DatabaseHelper::executeQuery($conn, $sql, 'i', $id);
        return $stmt ? true : false;
    }
}
?>
