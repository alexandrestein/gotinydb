package funcs

const (
	UserNameIndexName = "userName"
	AgeIndexName      = "age"
	CreationIndexName = "creation"
)

// func SetIndexes(t *testing.T, col *Collection) error {
// 	if err := col.SetIndex(UserNameIndexName, utils.StringComparatorType, []string{"UserName"}); err != nil {
// 		t.Error(err)
// 		return err
// 	}
// 	// Test duplicated index
// 	if err := col.SetIndex(UserNameIndexName, utils.StringComparatorType, []string{"UserName"}); err == nil {
// 		err = fmt.Errorf("there is no error but the index allready exist")
// 		t.Error(err)
// 		return err
// 	}
// 	if err := col.SetIndex(AgeIndexName, utils.IntComparatorType, []string{"Age"}); err != nil {
// 		t.Error(err)
// 		return err
// 	}
// 	if err := col.SetIndex(CreationIndexName, utils.TimeComparatorType, []string{"Creation"}); err != nil {
// 		t.Error(err)
// 		return err
// 	}
// 	return nil
// }
