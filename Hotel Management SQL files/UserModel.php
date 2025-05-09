<?php
require_once 'config/config.php';
require_once __DIR__ . '/../helpers/AuthHelper.php';


class UserModel {
    private static function executeQuery($sql, $types, ...$params) {
        global $conn;
        $stmt = mysqli_stmt_init($conn);
        if (!mysqli_stmt_prepare($stmt, $sql)) {
            throw new Exception('Failed to prepare statement.');
        }
        if ($types && $params) {
            mysqli_stmt_bind_param($stmt, $types, ...$params);
        }
        if (!mysqli_stmt_execute($stmt)) {
            throw new Exception('Failed to execute statement.');
        }
        return $stmt;
    }



    public static function unameExists($name) {
        $sql = "SELECT * FROM registered WHERE email = ?;";
        $stmt = self::executeQuery($sql, 's', $name);
        $result = mysqli_stmt_get_result($stmt);
        return mysqli_fetch_assoc($result) ?: false;
    }

    public static function createCustomer($fname, $lname, $email, $phone) {
        $sql = "INSERT INTO customer (fname, lname, email, phone) VALUES (?, ?, ?, ?);";
        $stmt = self::executeQuery($sql, 'ssss', $fname, $lname, $email, $phone);
        return mysqli_insert_id($GLOBALS['conn']);
    }

    public static function createUser($fname, $lname, $email, $phone, $pwd, $image) {
        try {
            $CustomerID = self::createCustomer($fname, $lname, $email, $phone);

            $sql = "INSERT INTO registered (CustomerID, password, image) VALUES (?, ?, ?);";
            $hashedPwd = password_hash($pwd, PASSWORD_DEFAULT);
            self::executeQuery($sql, 'iss', $CustomerID, $hashedPwd, $image);

            // Redirect to the index page after successful user creation
            header('Location: ../index.php?success=signup');
            exit();
        } catch (Exception $e) {
            error_log($e->getMessage());
            echo 'An error occurred. Please try again later.';
        }
    }

    public static function emailExists($email) {
        $sql = "SELECT customer.fname, customer.lname, customer.phone, customer.address, customer.country, customer.city, customer.email, registered.CustomerID, registered.password, registered.image FROM customer, registered WHERE customer.email = ? AND customer.CustomerID = registered.CustomerID;";
        $stmt = self::executeQuery($sql, 's', $email);
        $result = mysqli_stmt_get_result($stmt);
        return mysqli_fetch_assoc($result) ?: false;
    }

    public static function loginUser($email, $pwd) {
        try {
            // Rate limiting
            if (!isset($_SESSION['login_attempts'])) {
                $_SESSION['login_attempts'] = 0;
            }
            $_SESSION['login_attempts']++;
            if ($_SESSION['login_attempts'] > 5) {
                throw new Exception('Too many login attempts. Please try again later.');
            }

            $user = self::emailExists($email);
            if ($user === false) {
                throw new Exception('Email does not exist.');
            }

            $hashedPwd = $user['password'];
            if (!password_verify($pwd, $hashedPwd)) {
                throw new Exception('Incorrect password.');
            }

            // Reset login attempts on successful login
            $_SESSION['login_attempts'] = 0;

            // Start a secure session after password verification
            AuthHelper::startSecureSession(); // Ensure secure session handling

            $_SESSION['CustomerID'] = $user['CustomerID'];
            $_SESSION['fname'] = $user['fname'];
            $_SESSION['lname'] = $user['lname'];
            $_SESSION['email'] = $user['email'];
            $_SESSION['phone'] = $user['phone'];
            $_SESSION['address'] = $user['address'];
            $_SESSION['country'] = $user['country'];
            $_SESSION['city'] = $user['city'];
            $_SESSION['image'] = $user['image'];

            return true;
        } catch (Exception $e) {
            error_log($e->getMessage());
            echo $e->getMessage();
            return false;
        }
    }
}
