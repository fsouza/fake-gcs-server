package acl

type ACLEntity string
type ACLRole string

type ACLRule struct {
	Entity      ACLEntity    `json:"entity"`
	EntityID    string       `json:"entityId"`
	Role        ACLRole      `json:"role"`
	Domain      string       `json:"domain"`
	Email       string       `json:"email"`
	ProjectTeam *ProjectTeam `json:"projectTeam"`
}

// ProjectTeam is the project team associated with the entity, if any.
type ProjectTeam struct {
	ProjectNumber string `json:"projectNumber"`
	Team          string `json:"team"`
}
