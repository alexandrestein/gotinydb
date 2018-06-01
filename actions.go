package gotinydb

// NewAction returns a new Action pointer with the given ActionType
func NewAction(t ActionType) *Action {
	return &Action{
		Operation: t,
	}
}

// CompareTo defines the value you want to compare to
func (a *Action) CompareTo(val interface{}) *Action {
	a.CompareToValue = val
	return a
}

// GetType returns the type of the action given at the initialisation
func (a *Action) GetType() ActionType {
	return a.Operation
}

// EqualWanted defines if the exact corresponding key is retrieved or not.
func (a *Action) EqualWanted() *Action {
	a.KeepEqual = true
	return a
}

// SetSelector defines the configurable limit of IDs.
func (a *Action) SetSelector(s []string) *Action {
	a.Selector = s
	return a
}
