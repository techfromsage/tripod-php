<?php

declare(strict_types=1);

namespace Tripod\Mongo\Documents;

use MongoDB\Model\BSONDocument;

class Tables extends BSONDocument
{
    public function __construct(array $input = [])
    {
        parent::__construct($this->toTableRow($input));
    }

    /**
     * Sets the array value to the modeled table row value.
     *
     * @param array $data DB document array
     */
    public function bsonUnserialize(array $data): void
    {
        $this->exchangeArray($this->toTableRow($data));
    }

    /**
     * Models the table row from the source data.
     *
     * @param array $doc Database document
     *
     * @return array<string, mixed>
     */
    protected function toTableRow(array $doc)
    {
        $value = isset($doc['value']) && is_array($doc['value']) ? $doc['value'] : [];
        $result = [];
        foreach ($value as $key => $fieldValue) {
            $result[(string) $key] = $fieldValue;
        }
        if (isset($result[_IMPACT_INDEX])) {
            unset($result[_IMPACT_INDEX]);
        }

        return $result;
    }
}
