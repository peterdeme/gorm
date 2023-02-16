package tests_test

import (
	"database/sql"
	"os"
	"reflect"
	"testing"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	. "gorm.io/gorm/utils/tests"
)

func TestConnPoolWrapper(t *testing.T) {
	dialect := os.Getenv("GORM_DIALECT")
	if dialect != "mysql" {
		t.SkipNow()
	}

	dbDSN := os.Getenv("GORM_DSN")
	if dbDSN == "" {
		dbDSN = "gorm:gorm@tcp(localhost:9910)/gorm?charset=utf8&parseTime=True&loc=Local"
	}
	nativeDB, err := sql.Open("mysql", dbDSN)
	if err != nil {
		t.Fatalf("Should open db success, but got %v", err)
	}

	conn := &wrapperConnPool{
		db: nativeDB,
		expect: []string{
			"SELECT VERSION()",
			"INSERT INTO `users` (`created_at`,`updated_at`,`deleted_at`,`name`,`age`,`birthday`,`company_id`,`manager_id`,`active`) VALUES (?,?,?,?,?,?,?,?,?)",
			"SELECT * FROM `users` WHERE name = ? AND `users`.`deleted_at` IS NULL ORDER BY `users`.`id` LIMIT 1",
			"INSERT INTO `users` (`created_at`,`updated_at`,`deleted_at`,`name`,`age`,`birthday`,`company_id`,`manager_id`,`active`) VALUES (?,?,?,?,?,?,?,?,?)",
			"SELECT * FROM `users` WHERE name = ? AND `users`.`deleted_at` IS NULL ORDER BY `users`.`id` LIMIT 1",
			"SELECT * FROM `users` WHERE name = ? AND `users`.`deleted_at` IS NULL ORDER BY `users`.`id` LIMIT 1",
			"INSERT INTO `users` (`created_at`,`updated_at`,`deleted_at`,`name`,`age`,`birthday`,`company_id`,`manager_id`,`active`) VALUES (?,?,?,?,?,?,?,?,?)",
			"SELECT * FROM `users` WHERE name = ? AND `users`.`deleted_at` IS NULL ORDER BY `users`.`id` LIMIT 1",
			"SELECT * FROM `users` WHERE name = ? AND `users`.`deleted_at` IS NULL ORDER BY `users`.`id` LIMIT 1",
		},
	}

	defer func() {
		if !reflect.DeepEqual(conn.got, conn.expect) {
			t.Errorf("expect %#v but got %#v", conn.expect, conn.got)
		}
	}()

	db, err := gorm.Open(mysql.New(mysql.Config{Conn: conn, DisableWithReturning: true}))
	if err != nil {
		t.Fatalf("Should open db success, but got %v", err)
	}

	tx := db.Begin()
	user := *GetUser("transaction", Config{})

	if err = tx.Save(&user).Error; err != nil {
		t.Fatalf("No error should raise, but got %v", err)
	}

	if err = tx.First(&User{}, "name = ?", "transaction").Error; err != nil {
		t.Fatalf("Should find saved record, but got %v", err)
	}

	user1 := *GetUser("transaction1-1", Config{})

	if err = tx.Save(&user1).Error; err != nil {
		t.Fatalf("No error should raise, but got %v", err)
	}

	if err = tx.First(&User{}, "name = ?", user1.Name).Error; err != nil {
		t.Fatalf("Should find saved record, but got %v", err)
	}

	if sqlTx, ok := tx.Statement.ConnPool.(gorm.TxCommitter); !ok || sqlTx == nil {
		t.Fatalf("Should return the underlying sql.Tx")
	}

	tx.Rollback()

	if err = db.First(&User{}, "name = ?", "transaction").Error; err == nil {
		t.Fatalf("Should not find record after rollback, but got %v", err)
	}

	txDB := db.Where("fake_name = ?", "fake_name")
	tx2 := txDB.Session(&gorm.Session{NewDB: true}).Begin()
	user2 := *GetUser("transaction-2", Config{})
	if err = tx2.Save(&user2).Error; err != nil {
		t.Fatalf("No error should raise, but got %v", err)
	}

	if err = tx2.First(&User{}, "name = ?", "transaction-2").Error; err != nil {
		t.Fatalf("Should find saved record, but got %v", err)
	}

	tx2.Commit()

	if err = db.First(&User{}, "name = ?", "transaction-2").Error; err != nil {
		t.Fatalf("Should be able to find committed record, but got %v", err)
	}
}
