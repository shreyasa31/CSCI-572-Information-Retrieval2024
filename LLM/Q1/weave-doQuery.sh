echo '{
  "query": "{
    Get{
      SimSearch1 (
        limit: 3
        nearText: {
          concepts: [\"cricket\"],
        }
      ){
        question
        answer
        category
      }
    }
  }"
}'  | curl \
    -X POST \
    -H 'Content-Type: application/json' \
    -d @- \
    localhost:8080/v1/graphql 