package domain

import (
	"encoding/json"
	"log"
	"slices"
)

type Diff interface {
	Type() DiffType
	String() string
	Diff() map[string]string
}

type DiffType string

const (
	DiffTypeAddition DiffType = "addition"
	DiffTypeReplace  DiffType = "replacement"
	DiffTypeDeletion DiffType = "deletion"
)

func GetDiffTypeValues() []DiffType {
	return []DiffType{
		DiffTypeAddition,
		DiffTypeReplace,
		DiffTypeDeletion,
	}
}

func (dt *DiffType) IsValid() bool {
	if dt != nil && slices.Contains(GetDiffTypeValues(), *dt) {
		return true
	}

	return false
}

type Addition struct {
	Key   string
	Value string
}

func (Addition) Type() DiffType {
	return DiffTypeAddition
}

func (a Addition) String() string {
	str := struct {
		Type  string `json:"type"`
		Key   string `json:"key"`
		Value string `json:"value"`
	}{
		Type:  string(a.Type()),
		Key:   a.Key,
		Value: a.Value,
	}
	jsonBytes, err := json.Marshal(str)
	if err != nil {
		log.Println(err)
		return ""
	}
	return string(jsonBytes)
}

func (a Addition) Diff() map[string]string {
	return map[string]string{
		"key":   a.Key,
		"value": a.Value,
	}
}

type Replace struct {
	Key string
	New string
	Old string
}

func (Replace) Type() DiffType {
	return DiffTypeReplace
}

func (r Replace) String() string {
	str := struct {
		Type     string `json:"type"`
		Key      string `json:"key"`
		OldValue string `json:"old_value"`
		NewValue string `json:"new_value"`
	}{
		Type:     string(r.Type()),
		Key:      r.Key,
		OldValue: r.Old,
		NewValue: r.New,
	}
	jsonBytes, err := json.Marshal(str)
	if err != nil {
		log.Println(err)
		return ""
	}
	return string(jsonBytes)
}

func (r Replace) Diff() map[string]string {
	return map[string]string{
		"key":       r.Key,
		"old_value": r.Old,
		"new_value": r.New,
	}
}

type Deletion struct {
	Key   string
	Value string
}

func (Deletion) Type() DiffType {
	return DiffTypeDeletion
}

func (d Deletion) String() string {
	str := struct {
		Type  string `json:"type"`
		Key   string `json:"key"`
		Value string `json:"value"`
	}{
		Type:  string(d.Type()),
		Key:   d.Key,
		Value: d.Value,
	}
	jsonBytes, err := json.Marshal(str)
	if err != nil {
		log.Println(err)
		return ""
	}
	return string(jsonBytes)
}

func (d Deletion) Diff() map[string]string {
	return map[string]string{
		"key":   d.Key,
		"value": d.Value,
	}
}
