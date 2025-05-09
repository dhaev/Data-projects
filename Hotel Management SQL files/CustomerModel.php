<?php
require_once __DIR__ . '/../config/config.php';
require_once __DIR__ . '/../helpers/DatabaseHelper.php';

class CustomerModel {
    public static function getAllCustomers() {
        global $conn;
        $sql = "SELECT * FROM customer";
        $stmt = DatabaseHelper::executeQuery($conn, $sql, null);
        $result = mysqli_stmt_get_result($stmt);
        return mysqli_fetch_all($result, MYSQLI_ASSOC);
    }

    public static function getCustomerById($id) {
        global $conn;
        $sql = "SELECT * FROM customer WHERE CustomerID = ?";
        $stmt = DatabaseHelper::executeQuery($conn, $sql, 'i', $id);
        $result = mysqli_stmt_get_result($stmt);
        return mysqli_fetch_assoc($result);
    }

    public static function addCustomer($fname, $lname, $email, $phone) {
        global $conn;
        $sql = "INSERT INTO customer (fname, lname, email, phone) VALUES (?, ?, ?, ?)";
        $stmt = DatabaseHelper::executeQuery($conn, $sql, 'ssss', $fname, $lname, $email, $phone);
        return $stmt ? true : false;
    }

    public static function editCustomer($id, $fname, $lname, $email, $phone, $address = '', $country = '', $city = '') {
        global $conn;
        $sql = "UPDATE customer SET fname = ?, lname = ?, email = ?, phone = ?, address = ?, country = ?, city = ? WHERE CustomerID = ?";
        $stmt = DatabaseHelper::executeQuery($conn, $sql, 'sssssssi', $fname, $lname, $email, $phone, $address, $country, $city, $id);
        return $stmt ? true : false;
    }

    public static function deleteCustomer($id) {
        global $conn;
        $sql = "DELETE FROM customer WHERE CustomerID = ?";
        $stmt = DatabaseHelper::executeQuery($conn, $sql, 'i', $id);
        return $stmt ? true : false;
    }
}
?>
